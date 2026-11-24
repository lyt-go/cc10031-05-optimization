"""时间对齐领域服务：维护多测点时序按统一粒度与时区基准的时间对齐与插值规则。
读取标定后数据执行时间窗口对齐并写入对齐序列数据文件。
故障重采后按申请范围限定重算时格：迟到补采归格、排除留痕、复验关联与缺测检查。"""
from __future__ import annotations

import re
import time
from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class TimeAlignService(BaseModuleService):
    MODULE = "time_align"
    ENTITY = "对齐规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则编号", "对齐粒度", "时区基准", "插值方式", "容许偏移", "对齐条数", "未对齐条数", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "对齐粒度", "时区基准"]
    STATUS_ORDER = ["草稿", "待生效", "已生效", "已废弃"]
    ACTION_RULES = {"提交生效": "待生效", "确认生效": "已生效", "标记废弃": "已废弃"}
    # 数据清洗操作：按对齐粒度截断时间戳进行时序对齐并写入对齐序列文件
    NEGATIVE_ACTIONS = ["标记废弃"]

    # 重采复验时格状态：已确认=班次交接照旧值复算；待确认=覆盖不足待补；
    # 已接纳=纳入新观测待提交；已关联=提交复验后已与重采申请关联
    GRID_CONFIRMED = "已确认"
    GRID_PENDING = "待确认"
    GRID_ACCEPTED = "已接纳"
    GRID_LINKED = "已关联"

    def align_series(self, align_id: int, point_keys: list[str]) -> dict[str, Any]:
        """按对齐规则对多测点标定后的数据进行时间窗口对齐并写入对齐序列数据文件。"""
        rule = store.find(self.MODULE, align_id)
        if rule is None:
            return {"error": f"对齐规则 {align_id} 不存在"}
        granularity = rule.get("对齐粒度", "1s")
        # 读取各测点标定后的数据文件
        aligned = []
        unaligned_count = 0
        for pk in point_keys:
            cal_file = self._data_dir("calibrated") / f"{pk}_calibrated.jsonl"
            records = self._read_jsonl(cal_file)
            # 数据产物输出：写入对齐结果序列文件到数据目录
            for r in records:
                ts = r.get("timestamp", "")
                # 按对齐粒度截断时间戳
                aligned_ts = ts[:19]  # 截断到秒
                aligned.append({
                    "point": pk,
                    "aligned_timestamp": aligned_ts,
                    "source_timestamp": ts,
                    "value": r.get("calibrated_value_V", r.get("raw_value_mV", 0)),
                    "align_granularity": granularity,
                    "align_rule": rule["规则编号"],
                })
        # 写入时间对齐结果序列文件
        output_file = self._data_dir("aligned") / f"{rule['规则编号']}_aligned.jsonl"
        written = self._write_jsonl(output_file, aligned)
        rule["对齐条数"] = str(written)
        rule["未对齐条数"] = str(unaligned_count)
        return {"align_rule": rule["规则编号"], "points": len(point_keys),
                "aligned_count": written, "unaligned": unaligned_count}

    def detect_misaligned(self, point_key: str) -> dict[str, Any]:
        """检测指定测点序列中的时间不对齐数据点到异常日志字段文件。"""
        cal_file = self._data_dir("calibrated") / f"{point_key}_calibrated.jsonl"
        records = self._read_jsonl(cal_file)
        misaligned = []
        prev_ts = None
        for r in records:
            ts = r.get("timestamp", "")
            if prev_ts and ts < prev_ts:
                misaligned.append(r)
            prev_ts = ts
        # 将未对齐数据写入异常检测数据文件
        if misaligned:
            self._write_jsonl(
                self._data_dir("aligned") / f"{point_key}_misaligned.jsonl",
                misaligned
            )
        return {"point": point_key, "total": len(records),
                "misaligned_count": len(misaligned)}

    # ---- 故障重采后的重算范围：复验会话 ----

    def start_recheck(self, align_id: int, replay_id: int,
                      handover_grids: dict[str, Any] | None = None,
                      min_observations: int = 1,
                      reference_date: str | None = None) -> dict[str, Any]:
        """开始复验：按重采申请范围与采样当时有效的对齐粒度生成目标时间格。

        已做班次确认的时格登记旧交接值（复算时照旧值），重采产物记录先行归格；
        范围外记录直接排除并保留原因，不作为本次补测。
        """
        rule = store.find(self.MODULE, align_id)
        if rule is None:
            return {"error": f"对齐规则 {align_id} 不存在"}
        replay = store.find("range_replay", replay_id)
        if replay is None:
            return {"error": f"重采任务 {replay_id} 不存在"}
        start_ts = self._parse_ts(replay.get("起始时刻"), reference_date)
        end_ts = self._parse_ts(replay.get("截止时刻"), reference_date)
        if start_ts is None or end_ts is None or end_ts < start_ts:
            return {"error": "重采申请的起止时刻无法解析"}
        session = {
            "align_id": align_id, "replay_id": replay_id,
            "rule_no": rule["规则编号"], "replay_no": replay["重采编号"],
            "point": replay.get("测点编号", ""),
            "range_start": start_ts, "range_end": end_ts,
            "min_observations": max(1, int(min_observations)),
            "status": "复验中",
            "grids": {}, "excluded": [],
        }
        # 已交接确认的时格：登记旧交接值与交接号，复算照旧值不纳入新观测
        for key, meta in (handover_grids or {}).items():
            ts = self._parse_ts(key, reference_date)
            if ts is None:
                continue
            grid_key = self._grid_of(rule, ts)
            if isinstance(meta, dict):
                value, ref = meta.get("value"), str(meta.get("交接号", ""))
            else:
                value, ref = meta, ""
            session["grids"][grid_key] = {
                "status": self.GRID_CONFIRMED, "observations": [],
                "handover_value": value, "handover_ref": ref,
            }
        # 重采产物记录按采样当时有效的粒度归格
        replay_file = self._data_dir("replayed_ranges") / f"{replay['重采编号']}_replay.jsonl"
        for rec in self._read_jsonl(replay_file):
            self._bucket_record(session, rule, rec)
        self._save_recheck(session)
        view = self._recheck_view(session, rule)
        view["range"] = {"start": self._fmt_ts(start_ts), "end": self._fmt_ts(end_ts)}
        return view

    def accept_late_records(self, align_id: int, replay_id: int,
                            records: list[dict[str, Any]]) -> dict[str, Any]:
        """提交复验前接纳迟到的补采记录：归入目标时间格，范围外或已交接的排除并记录原因。"""
        session, rule, _, err = self._load_session(align_id, replay_id)
        if err:
            return {"error": err}
        if session["status"] != "复验中":
            return {"error": "复验已提交，迟到补采须发起下一次复验"}
        accepted = excluded = 0
        for rec in records:
            if self._bucket_record(session, rule, rec):
                accepted += 1
            else:
                excluded += 1
        self._save_recheck(session)
        view = self._recheck_view(session, rule)
        view["accepted_now"] = accepted
        view["excluded_now"] = excluded
        return view

    def submit_recheck(self, align_id: int, replay_id: int) -> dict[str, Any]:
        """提交复验：已确认时格照旧交接值复算，已接纳时格用新观测校验并与重采申请关联。"""
        session, rule, replay, err = self._load_session(align_id, replay_id)
        if err:
            return {"error": err}
        if session["status"] != "复验中":
            return {"error": "复验已提交，不能重复提交"}
        linked, recalculated = [], []
        for grid_key in self._enum_grids(session, rule):
            grid = session["grids"].get(grid_key)
            if grid is None:
                continue
            if grid["status"] == self.GRID_CONFIRMED:
                # 已经确认的统计照旧交接值复算
                grid["value"] = grid.get("handover_value")
                recalculated.append({
                    "grid": grid_key, "label": grid_key[11:16],
                    "value": grid.get("handover_value"), "source": "旧交接值复算",
                    "handover_ref": grid.get("handover_ref", ""),
                })
            elif grid["status"] == self.GRID_ACCEPTED:
                # 待确认区间用新观测继续校验，取格内观测均值
                obs = grid["observations"]
                grid["value"] = round(sum(float(o["value"]) for o in obs) / len(obs), 4)
                grid["status"] = self.GRID_LINKED
                linked.append(grid_key)
        # 提交复验后：已接纳时格与重采申请关联，写入关联产物文件
        if linked:
            self._write_jsonl(
                self._data_dir("aligned") / f"{rule['规则编号']}_replay_link.jsonl",
                [{"replay_id": session["replay_no"], "rule": session["rule_no"],
                  "grid": g, "value": session["grids"][g].get("value"),
                  "observations": len(session["grids"][g]["observations"]),
                  "linked_at": time.time()} for g in linked])
        accepted_points = sum(len(session["grids"][g]["observations"]) for g in linked)
        replay["重采点数"] = str(accepted_points)
        replay["重采水位"] = str(len(linked))
        replay["关联时格"] = ",".join(g[11:16] for g in linked)
        rule["对齐条数"] = str(accepted_points)
        rule["未对齐条数"] = str(len(session["excluded"]))
        session["status"] = "已提交"
        session["submitted_at"] = time.time()
        self._save_recheck(session)
        return {"replay_id": session["replay_no"], "rule": session["rule_no"],
                "linked_grids": [g[11:16] for g in linked],
                "accepted_points": accepted_points,
                "recalculated": recalculated,
                "excluded": len(session["excluded"])}

    def missing_check(self, align_id: int, replay_id: int,
                      include_next: bool = True) -> dict[str, Any]:
        """缺测检查：核对本次复验各时格状态、缺测原因与被排除记录，并扫描下一段缺测时格。"""
        session, rule, _, err = self._load_session(align_id, replay_id)
        if err:
            return {"error": err}
        grids = self._grid_report(session, rule)
        pending = [g["label"] for g in grids if g["pending"]]
        recalculated = [{
            "grid": g["grid"], "label": g["label"], "value": g.get("value"),
            "source": g.get("source", ""), "handover_ref": g.get("handover_ref", ""),
        } for g in grids if g["status"] == self.GRID_CONFIRMED]
        result: dict[str, Any] = {
            "replay_id": session["replay_no"], "rule": session["rule_no"],
            "recheck_status": session["status"],
            "grids": grids, "pending_grids": pending,
            "excluded": list(session["excluded"]),
            "recalculated": recalculated,
        }
        if include_next:
            result["next_segment"] = self._next_segment(session, rule)
        # 缺测检查报告写入对齐产物目录
        self._write_jsonl(
            self._data_dir("aligned") / f"{rule['规则编号']}_missing_check.jsonl",
            [{"replay_id": session["replay_no"], "checked_at": time.time(),
              "pending_grids": pending, "excluded": len(session["excluded"]),
              "next_segment_missing": len(result.get("next_segment", {}).get("grids", []))}])
        return result

    # ---- 复验会话内部工具 ----

    @staticmethod
    def _recheck_name(rule_no: str, replay_no: str) -> str:
        return f"recheck_{rule_no}_{replay_no}"

    def _save_recheck(self, session: dict[str, Any]) -> None:
        self._write_checkpoint(self._recheck_name(session["rule_no"], session["replay_no"]), session)

    def _load_session(self, align_id: int, replay_id: int):
        """读取复验会话；返回 (session, rule, replay, error)。"""
        rule = store.find(self.MODULE, align_id)
        if rule is None:
            return None, None, None, f"对齐规则 {align_id} 不存在"
        replay = store.find("range_replay", replay_id)
        if replay is None:
            return None, None, None, f"重采任务 {replay_id} 不存在"
        session = self._read_checkpoint(self._recheck_name(rule["规则编号"], replay["重采编号"]))
        if session is None:
            return None, None, None, "复验会话不存在，请先开始复验"
        return session, rule, replay, None

    def _bucket_record(self, session: dict[str, Any], rule: dict[str, Any],
                       rec: dict[str, Any]) -> bool:
        """把一条重采记录归入目标时间格；不纳入时把原因写进 excluded。"""
        raw_ts = rec.get("timestamp") or rec.get("original_timestamp")
        ts = self._parse_ts(raw_ts)
        point = rec.get("point") or session.get("point", "")
        value = rec.get("value", rec.get("replay_value",
                                         rec.get("calibrated_value_V", rec.get("raw_value_mV"))))
        if ts is None:
            self._exclude(session, raw_ts, point, rec.get("value"), "时间戳无法解析，不归入任何时格")
            return False
        if value is None:
            self._exclude(session, self._fmt_ts(ts), point, None, "缺少观测值，不纳入任何时格")
            return False
        try:
            float(value)
        except (TypeError, ValueError):
            self._exclude(session, self._fmt_ts(ts), point, value, "观测值无法解析，不纳入任何时格")
            return False
        if ts < session["range_start"] or ts > session["range_end"]:
            self._exclude(
                session, self._fmt_ts(ts), point, value,
                f"范围外：采样时刻{self._fmt_ts(ts)}不在重采申请"
                f"{self._fmt_ts(session['range_start'])}至{self._fmt_ts(session['range_end'])}内，"
                "不作为本次补测")
            return False
        grid_key = self._grid_of(rule, ts)
        grid = session["grids"].setdefault(
            grid_key, {"status": self.GRID_PENDING, "observations": []})
        if grid["status"] == self.GRID_CONFIRMED:
            self._exclude(session, self._fmt_ts(ts), point, value,
                          f"已交接确认：时格{grid_key[11:16]}按旧交接值复算，不纳入本次补采")
            return False
        grid["observations"].append({
            "timestamp": self._fmt_ts(ts), "point": point, "value": float(value)})
        need = session["min_observations"]
        grid["status"] = self.GRID_ACCEPTED if len(grid["observations"]) >= need else self.GRID_PENDING
        return True

    def _exclude(self, session: dict[str, Any], timestamp: Any, point: Any,
                 value: Any, reason: str) -> None:
        """保留为何不纳入某条记录。"""
        session["excluded"].append({
            "timestamp": timestamp, "point": point, "value": value, "reason": reason})

    def _recheck_view(self, session: dict[str, Any], rule: dict[str, Any]) -> dict[str, Any]:
        return {
            "replay_id": session["replay_no"], "rule": session["rule_no"],
            "recheck_status": session["status"],
            "grids": self._grid_report(session, rule),
            "excluded": list(session["excluded"]),
        }

    def _grid_report(self, session: dict[str, Any], rule: dict[str, Any]) -> list[dict[str, Any]]:
        """按重算范围逐格汇总状态：待补格给出覆盖不足原因，已确认格标注旧交接值来源。"""
        report = []
        need = session["min_observations"]
        for grid_key in self._enum_grids(session, rule):
            grid = session["grids"].get(grid_key)
            label = grid_key[11:16]
            if grid is None or grid["status"] == self.GRID_PENDING:
                n = len(grid["observations"]) if grid else 0
                report.append({
                    "grid": grid_key, "label": label, "status": "待补", "pending": True,
                    "observations": n,
                    "reason": f"覆盖不足：观测{n}条，未达到每格至少{need}条"})
            elif grid["status"] == self.GRID_CONFIRMED:
                report.append({
                    "grid": grid_key, "label": label, "status": self.GRID_CONFIRMED,
                    "pending": False, "value": grid.get("handover_value"),
                    "source": "旧交接值复算", "handover_ref": grid.get("handover_ref", "")})
            else:
                report.append({
                    "grid": grid_key, "label": label, "status": grid["status"],
                    "pending": False, "value": grid.get("value"),
                    "observations": len(grid["observations"])})
        return report

    def _next_segment(self, session: dict[str, Any], rule: dict[str, Any]) -> dict[str, Any]:
        """下一段缺测检查：本次范围之后的等长时段逐格标记缺测。"""
        end = session["range_end"]
        span = end - session["range_start"]
        grids = []
        cursor = end
        while cursor < end + span:
            gran = self._granularity_at(rule, cursor + 1)
            cursor = cursor - (cursor % gran) + gran
            if cursor <= end + span:
                grids.append({
                    "grid": self._fmt_ts(cursor), "label": self._fmt_ts(cursor)[11:16],
                    "status": "缺测", "pending": True,
                    "reason": "下一段尚未覆盖，待补测"})
        return {"start": self._fmt_ts(end), "end": self._fmt_ts(end + span), "grids": grids}

    def _enum_grids(self, session: dict[str, Any], rule: dict[str, Any]) -> list[str]:
        """枚举重算范围内的目标时间格（粒度随采样当时有效的规则沿革变化）。"""
        grids = []
        ts = session["range_start"]
        gran = self._granularity_at(rule, ts)
        ts = ts - (ts % gran)
        while ts <= session["range_end"]:
            grids.append(self._fmt_ts(ts))
            gran = self._granularity_at(rule, ts + 1)
            if gran <= 0:
                break
            ts = ts - (ts % gran) + gran
        return grids

    def _grid_of(self, rule: dict[str, Any], ts: float) -> str:
        """按采样当时有效的对齐粒度把时刻归入目标时间格。"""
        gran = self._granularity_at(rule, ts)
        return self._fmt_ts(ts - (ts % gran))

    def _granularity_at(self, rule: dict[str, Any], ts: float) -> int:
        """采样当时有效的对齐粒度（秒）：取粒度沿革中生效起点不晚于 ts 的最近一档。"""
        best = None
        for item in rule.get("粒度沿革") or []:
            eff = self._parse_ts(item.get("生效起点"))
            if eff is not None and eff <= ts and (best is None or eff > best[0]):
                best = (eff, item.get("对齐粒度"))
        return self._granularity_seconds(best[1] if best else rule.get("对齐粒度"))

    @staticmethod
    def _granularity_seconds(granularity: Any) -> int:
        s = str(granularity or "").strip().lower()
        m = re.fullmatch(r"(\d+)\s*(s|sec|m|min|h)", s)
        if not m:
            return 1
        n = max(1, int(m.group(1)))
        unit = m.group(2)
        if unit in ("m", "min"):
            return n * 60
        if unit == "h":
            return n * 3600
        return n

    @staticmethod
    def _fmt_ts(ts: float) -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(ts))

    def _parse_ts(self, ts: Any, reference_date: str | None = None) -> float | None:
        """解析时间戳为 epoch 秒；支持完整 ISO 与 HH:MM（配合基准日期）。"""
        s = str(ts or "").strip()
        if not s:
            return None
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(s[:26], fmt).timestamp()
            except ValueError:
                continue
        for fmt in ("%H:%M:%S", "%H:%M"):
            try:
                parsed = datetime.strptime(s, fmt)
            except ValueError:
                continue
            try:
                base = datetime.strptime(reference_date, "%Y-%m-%d") if reference_date else datetime.now()
            except (ValueError, TypeError):
                base = datetime.now()
            return parsed.replace(year=base.year, month=base.month, day=base.day).timestamp()
        return None