"""SQLite 持久化：实验文档、快照、分叉全部落盘。

导入/导出是整个实验（含所有分支）的 JSON 快照，重放纯由虚拟时钟
驱动，因此重启或导入后 fence token、处理顺序与最终摘要完全一致。
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
from typing import Any, Dict, List, Optional


SCHEMA = """
CREATE TABLE IF NOT EXISTS experiments (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    doc         TEXT NOT NULL,
    parent_id   TEXT,
    fork_after  INTEGER,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS snapshots (
    id          TEXT PRIMARY KEY,
    experiment  TEXT NOT NULL,
    label       TEXT NOT NULL,
    after_k     INTEGER NOT NULL,
    payload     TEXT NOT NULL,
    created_at  INTEGER NOT NULL,
    FOREIGN KEY(experiment) REFERENCES experiments(id)
);
"""


class Store:
    def __init__(self, path: str) -> None:
        self.path = path
        parent = os.path.dirname(os.path.abspath(path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    # ---- 实验 ----
    def list_experiments(self) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT id, name, parent_id, fork_after, created_at, updated_at "
            "FROM experiments ORDER BY created_at").fetchall()
        return [dict(r) for r in rows]

    def create_experiment(self, exp_id: str, name: str,
                          doc: Dict[str, Any], parent_id: Optional[str] = None,
                          fork_after: Optional[int] = None) -> Dict[str, Any]:
        now = int(time.time() * 1000)
        self.conn.execute(
            "INSERT INTO experiments (id, name, doc, parent_id, fork_after, "
            "created_at, updated_at) VALUES (?,?,?,?,?,?,?)",
            (exp_id, name, json.dumps(doc, ensure_ascii=False, sort_keys=True),
             parent_id, fork_after, now, now))
        self.conn.commit()
        return {"id": exp_id, "name": name, "parent_id": parent_id,
                "fork_after": fork_after, "created_at": now,
                "updated_at": now}

    def get_experiment(self, exp_id: str) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            "SELECT * FROM experiments WHERE id=?", (exp_id,)).fetchone()
        if not row:
            return None
        d = dict(row)
        d["doc"] = json.loads(d["doc"])
        return d

    def get_doc(self, exp_id: str) -> Dict[str, Any]:
        exp = self.get_experiment(exp_id)
        if exp is None:
            raise KeyError(exp_id)
        return exp["doc"]

    def save_doc(self, exp_id: str, doc: Dict[str, Any],
                 name: Optional[str] = None) -> None:
        now = int(time.time() * 1000)
        if name is not None:
            self.conn.execute(
                "UPDATE experiments SET doc=?, name=?, updated_at=? WHERE id=?",
                (json.dumps(doc, ensure_ascii=False, sort_keys=True),
                 name, now, exp_id))
        else:
            self.conn.execute(
                "UPDATE experiments SET doc=?, updated_at=? WHERE id=?",
                (json.dumps(doc, ensure_ascii=False, sort_keys=True),
                 now, exp_id))
        self.conn.commit()

    def delete_experiment(self, exp_id: str) -> None:
        self.conn.execute("DELETE FROM snapshots WHERE experiment=?",
                          (exp_id,))
        self.conn.execute("DELETE FROM experiments WHERE id=?", (exp_id,))
        self.conn.commit()

    # ---- 快照 ----
    def save_snapshot(self, snap_id: str, exp_id: str, label: str,
                      after_k: int, payload: Dict[str, Any]) -> None:
        self.conn.execute(
            "INSERT INTO snapshots (id, experiment, label, after_k, payload, "
            "created_at) VALUES (?,?,?,?,?,?)",
            (snap_id, exp_id, label, after_k,
             json.dumps(payload, ensure_ascii=False, sort_keys=True),
             int(time.time() * 1000)))
        self.conn.commit()

    def list_snapshots(self, exp_id: str) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT id, experiment, label, after_k, created_at FROM snapshots "
            "WHERE experiment=? ORDER BY created_at", (exp_id,)).fetchall()
        return [dict(r) for r in rows]

    def get_snapshot(self, snap_id: str) -> Optional[Dict[str, Any]]:
        row = self.conn.execute("SELECT * FROM snapshots WHERE id=?",
                                (snap_id,)).fetchone()
        if not row:
            return None
        d = dict(row)
        d["payload"] = json.loads(d["payload"])
        return d

    # ---- 导入 / 导出 ----
    def export_all(self) -> Dict[str, Any]:
        exps = []
        for row in self.conn.execute("SELECT * FROM experiments").fetchall():
            d = dict(row)
            d["doc"] = json.loads(d["doc"])
            exps.append(d)
        snaps = []
        for row in self.conn.execute("SELECT * FROM snapshots").fetchall():
            d = dict(row)
            d["payload"] = json.loads(d["payload"])
            snaps.append(d)
        return {"format": "leasedebug-export", "version": 1,
                "exported_at": int(time.time() * 1000),
                "experiments": exps, "snapshots": snaps}

    def import_all(self, data: Dict[str, Any]) -> Dict[str, int]:
        if data.get("format") != "leasedebug-export":
            raise ValueError("不认识的导出格式")
        n_exp = n_snap = 0
        for e in data.get("experiments", []):
            self.conn.execute(
                "INSERT OR REPLACE INTO experiments (id, name, doc, "
                "parent_id, fork_after, created_at, updated_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (e["id"], e["name"],
                 json.dumps(e["doc"], ensure_ascii=False, sort_keys=True),
                 e.get("parent_id"), e.get("fork_after"),
                 e["created_at"], e["updated_at"]))
            n_exp += 1
        for s in data.get("snapshots", []):
            self.conn.execute(
                "INSERT OR REPLACE INTO snapshots (id, experiment, label, "
                "after_k, payload, created_at) VALUES (?,?,?,?,?,?)",
                (s["id"], s["experiment"], s["label"], s["after_k"],
                 json.dumps(s["payload"], ensure_ascii=False, sort_keys=True),
                 s["created_at"]))
            n_snap += 1
        self.conn.commit()
        return {"experiments": n_exp, "snapshots": n_snap}
