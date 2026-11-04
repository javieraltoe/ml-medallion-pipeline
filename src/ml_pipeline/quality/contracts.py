"""Validación de contratos de datos (schema contracts) sin dependencias externas.

Política:
  * campo requerido ausente o de tipo incorrecto  -> violación
  * campo nuevo no declarado en el contrato        -> warning (schema drift), no rompe
  * si % de registros con violaciones > umbral     -> SchemaContractError
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

CONTRACTS_DIR = Path(__file__).resolve().parents[3] / "contracts"

_TYPES: dict[str, tuple[type, ...]] = {
    "string": (str,),
    "number": (int, float),
    "integer": (int,),
    "boolean": (bool,),
    "object": (dict,),
    "array": (list,),
}


class SchemaContractError(ValueError):
    pass


@dataclass
class ContractReport:
    contract: str
    total: int = 0
    invalid_records: int = 0
    violations: dict[str, int] = field(default_factory=dict)  # "campo: problema" -> conteo
    unknown_fields: set[str] = field(default_factory=set)

    @property
    def violation_ratio(self) -> float:
        return self.invalid_records / self.total if self.total else 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "contract": self.contract,
            "total": self.total,
            "invalid_records": self.invalid_records,
            "violation_ratio": round(self.violation_ratio, 4),
            "violations": self.violations,
            "unknown_fields": sorted(self.unknown_fields),
        }


def load_contract(name: str, contracts_dir: Path | None = None) -> dict[str, Any]:
    directory = contracts_dir or Path(os.getenv("CONTRACTS_DIR", str(CONTRACTS_DIR)))
    return json.loads((Path(directory) / f"{name}.json").read_text())


def _type_ok(value: Any, expected: str) -> bool:
    if expected in ("number", "integer") and isinstance(value, bool):
        return False  # bool es subclase de int en Python
    return isinstance(value, _TYPES[expected])


def validate_records(records: list[dict[str, Any]], contract: dict[str, Any]) -> ContractReport:
    spec: dict[str, dict[str, Any]] = contract["fields"]
    report = ContractReport(contract=f"{contract['name']}@{contract['version']}", total=len(records))

    for rec in records:
        bad = False
        for fname, rule in spec.items():
            if fname not in rec:
                if rule.get("required"):
                    report.violations[f"{fname}: ausente"] = report.violations.get(f"{fname}: ausente", 0) + 1
                    bad = True
                continue
            value = rec[fname]
            if value is None:
                if not rule.get("nullable"):
                    key = f"{fname}: nulo no permitido"
                    report.violations[key] = report.violations.get(key, 0) + 1
                    bad = True
                continue
            if not _type_ok(value, rule["type"]):
                key = f"{fname}: se esperaba {rule['type']}"
                report.violations[key] = report.violations.get(key, 0) + 1
                bad = True
        report.unknown_fields.update(set(rec) - set(spec))
        report.invalid_records += bad
    return report


def enforce(report: ContractReport, max_violation_ratio: float) -> None:
    if report.unknown_fields:
        log.warning("Schema drift: campos nuevos no declarados %s", sorted(report.unknown_fields))
    if report.violation_ratio > max_violation_ratio:
        raise SchemaContractError(
            f"Contrato {report.contract} incumplido: {report.violation_ratio:.1%} de registros inválidos "
            f"(umbral {max_violation_ratio:.0%}). Detalle: {report.violations}"
        )
