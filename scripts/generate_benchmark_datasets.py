#!/usr/bin/env python3

import argparse
import csv
from pathlib import Path


PERSON_HEADERS = [
    "Instcode",
    "Unieknr",
    "Geslacht",
    "Gebdat",
    "Nationaliteit",
    "BKO",
    "SKO",
    "Peildat",
]

APPOINTMENT_HEADERS = [
    "Instcode",
    "Unieknr",
    "Volgnrar",
    "Aadnstvb",
    "Ingdatdv",
    "Eindatdv",
    "HOOPgeb",
    "Functcat",
    "WelGeenHGL",
    "Salschal",
    "Taakomv",
    "Aanst_22",
    "Peildat",
]

HOOPGEB_VALUES = ["ECON", "G&M", "RECH", "MED", "SCI", "HUM"]
FUNCTCAT_VALUES = ["10940", "10620", "100910", "10930", "10720", "70120"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate deterministic benchmark CSV tables for DYNAMOS sql-query images."
    )
    parser.add_argument("--rows", type=int, required=True, help="Rows per provider/table")
    parser.add_argument(
        "--providers",
        default="UVA,VU",
        help="Comma-separated provider names, for example UVA,VU",
    )
    parser.add_argument("--output-dir", required=True, help="Directory to write generated CSVs into")
    return parser.parse_args()


def provider_code(provider: str) -> str:
    normalized = provider.strip().upper()
    return (normalized[:3] or "XXX").ljust(3, "X")


def birth_date(index: int) -> str:
    year = 1970 + (index % 35)
    month = 1 + (index % 12)
    day = 1 + (index % 28)
    return f"{year:04d}{month:02d}{day:02d}"


def ingangsdatum(index: int) -> str:
    year = 2004 + (index % 18)
    month = 1 + ((index // 3) % 12)
    day = 1 + ((index // 7) % 28)
    return f"{year:04d}{month:02d}{day:02d}"


def peildatum(index: int) -> str:
    return "20201231" if index % 2 else "20191231"


def personen_row(instcode: str, unique_number: int, index: int) -> list[str]:
    return [
        instcode,
        str(unique_number),
        "V" if index % 2 == 0 else "M",
        birth_date(index),
        f"{index % 250:04d}",
        str(index % 2),
        str((index // 2) % 2),
        peildatum(index),
    ]


def aanstellingen_row(instcode: str, unique_number: int, index: int) -> list[str]:
    return [
        instcode,
        str(unique_number),
        "1",
        "V",
        ingangsdatum(index),
        "99990101",
        HOOPGEB_VALUES[index % len(HOOPGEB_VALUES)],
        FUNCTCAT_VALUES[index % len(FUNCTCAT_VALUES)],
        "0",
        str(8 + (index % 9)),
        str(1000 + ((index % 10) * 1000)),
        "V",
        peildatum(index),
    ]


def generate_provider_tables(output_dir: Path, provider: str, rows: int, provider_index: int) -> None:
    instcode = provider_code(provider)
    start_number = (provider_index + 1) * 10_000_000
    personen_path = output_dir / f"PersonenLarge_{provider.upper()}.csv"
    aanstellingen_path = output_dir / f"AanstellingenLarge_{provider.upper()}.csv"

    with personen_path.open("w", newline="", encoding="utf-8") as personen_handle, aanstellingen_path.open(
        "w", newline="", encoding="utf-8"
    ) as aanstellingen_handle:
        personen_writer = csv.writer(personen_handle, delimiter=";")
        aanstellingen_writer = csv.writer(aanstellingen_handle, delimiter=";")
        personen_writer.writerow(PERSON_HEADERS)
        aanstellingen_writer.writerow(APPOINTMENT_HEADERS)

        for row_index in range(rows):
            unique_number = start_number + row_index + 1
            personen_writer.writerow(personen_row(instcode, unique_number, row_index))
            aanstellingen_writer.writerow(aanstellingen_row(instcode, unique_number, row_index))


def main() -> int:
    args = parse_args()
    if args.rows <= 0:
        raise SystemExit("--rows must be greater than zero")

    providers = [provider.strip().upper() for provider in args.providers.split(",") if provider.strip()]
    if not providers:
        raise SystemExit("--providers must contain at least one provider")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for provider_index, provider in enumerate(providers):
        generate_provider_tables(output_dir, provider, args.rows, provider_index)

    print(
        f"Generated benchmark SQL tables for {', '.join(providers)} with {args.rows} rows per provider in {output_dir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())