
import re

# -----------------------------
# Normalisation helpers
# -----------------------------
def _norm(s: str) -> str:
    return str(s or "").strip().upper()

def _normalize_time_any(s: str) -> str:
    """17H15 / 08h15 / 22h56 / 16:55 -> HH:MM"""
    s0 = str(s or "")
    m = re.search(r"\b(\d{1,2})\s*[:hH]\s*(\d{2})\b", s0)
    if m:
        hh, mm = m.groups()
        return f"{int(hh):02d}:{int(mm):02d}"
    return ""

def _normalize_date_to_iso(s: str, default_year: int = 2026) -> str:
    """
    Support:
      - 05.03.2026 / 05/03/2026 / 05-03-26
      - Sa 21/03 / lundi 02/03 / Date : 02/03
      - 12 mars 26
    Returns YYYY-MM-DD or "".
    """
    s0 = str(s or "").strip()
    if not s0:
        return ""

    # dd.mm.yyyy | dd/mm/yy | dd-mm-yyyy
    m = re.search(r"(\d{1,2})[\.\/\-](\d{1,2})[\.\/\-](\d{2,4})", s0)
    if m:
        d, mo, y = m.groups()
        y = int(y)
        if y < 100:
            y += 2000
        return f"{y:04d}-{int(mo):02d}-{int(d):02d}"

    # dayname dd/mm (no year)
    m2 = re.search(r"\b(\d{1,2})[\/\-](\d{1,2})\b", s0)
    if m2:
        d, mo = m2.groups()
        return f"{default_year:04d}-{int(mo):02d}-{int(d):02d}"

    # FR month words
    months = {
        "JANVIER": 1, "JANV": 1,
        "FEVRIER": 2, "FÉVRIER": 2, "FEVR": 2, "FÉVR": 2,
        "MARS": 3,
        "AVRIL": 4, "AVR": 4,
        "MAI": 5,
        "JUIN": 6,
        "JUILLET": 7, "JUIL": 7,
        "AOUT": 8, "AOÛT": 8,
        "SEPTEMBRE": 9, "SEPT": 9,
        "OCTOBRE": 10, "OCT": 10,
        "NOVEMBRE": 11, "NOV": 11,
        "DECEMBRE": 12, "DÉCEMBRE": 12, "DEC": 12, "DÉC": 12,
    }
    m3 = re.search(r"(\d{1,2})\s+([A-Za-zÉÈÊËÀÂÄÔÖÛÜÙÇéèêëàâäôöûüùç]+)\s+(\d{2,4})", s0, re.IGNORECASE)
    if m3:
        d, mon, y = m3.groups()
        mon_u = _norm(mon)
        mon_u = (mon_u
                 .replace("É","E").replace("È","E").replace("Ê","E").replace("Ë","E")
                 .replace("À","A").replace("Â","A").replace("Ä","A")
                 .replace("Ô","O").replace("Ö","O")
                 .replace("Û","U").replace("Ü","U").replace("Ù","U")
                 .replace("Ç","C"))
        mo = months.get(mon_u) or months.get(mon_u[:4]) or months.get(mon_u[:3])
        y = int(y)
        if y < 100:
            y += 2000
        if mo:
            return f"{y:04d}-{int(mo):02d}-{int(d):02d}"
    return ""

def _split_addr_cp_city(full: str):
    s = re.sub(r"\s+", " ", str(full or "")).strip()
    if not s:
        return "", "", ""
    # remove prefixes
    for pref in ["DOMICILE :", "DOMICILE:", "Domicile :", "Domicile:", "Adresse :", "Adresse:"]:
        if s.startswith(pref):
            s = s[len(pref):].strip()

    m = re.search(r"\b(\d{4,5})\s+([A-Za-zÀ-ÿ\-\']+(?:\s+[A-Za-zÀ-ÿ\-\']+)*)$", s)
    if m:
        cp = m.group(1).strip()
        city = m.group(2).strip()
        adr = s[:m.start()].strip(" ,")
        return adr, cp, city
    return s, "", ""

def _extract_phone(text: str) -> str:
    t = str(text or "")
    # +3247... or 003247... or 047...
    m = re.search(r"(\+?\d{2,3}\s?\d[\d\s]{6,}\d)", t)
    if m:
        return re.sub(r"\s+", "", m.group(1)).strip()
    return ""

def _extract_flight(text: str) -> str:
    t = str(text or "").upper()
    # prefer patterns inside parentheses
    m = re.search(r"\(([A-Z]{1,3}\d{2,5})\)", t)
    if m:
        return m.group(1).strip()
    # fallback
    m2 = re.search(r"\b([A-Z]{1,3}\d{2,5})\b", t)
    if m2:
        return m2.group(1).strip()
    return ""

def _detect_airport_and_place(text: str):
    """
    Returns (designation_code, place_hint)
    designation_code: BRU/CRL/LUX/ZAV/DUS/MIDI/...
    place_hint: text for pickup/dest if needed
    """
    u = _norm(text)
    # Midi first
    if "MIDI" in u and ("BRUX" in u or "BRUS" in u or "BRUSSELS" in u):
        return "MIDI", "BRUXELLES MIDI"
    if "BRUXELLES MIDI" in u or "BRUSSELS MIDI" in u:
        return "MIDI", "BRUXELLES MIDI"

    if "ZAV" in u or "ZAVENTEM" in u:
        return "ZAV", "ZAVENTEM"
    if "BRU" in u or "BRUSSELS AIRPORT" in u or "BRUXELLES AIRPORT" in u or "BRUSSELS  AIRPORT" in u:
        return "BRU", "BRUSSELS AIRPORT"
    if "CHARLEROI" in u or "CRL" in u:
        return "CRL", "CHARLEROI"
    if "LUXEMBOURG" in u or "LUX" in u:
        return "LUX", "LUXEMBOURG"
    if "DUESSELDORF" in u or "DUSSELDORF" in u or "DÜSSELDORF" in u or "DUS" in u:
        return "DUS", "DUSSELDORF"
    return "", ""

def _detect_sens(block_text: str) -> str:
    u = _norm(block_text)
    # return patterns
    if ("FROM" in u and "AIRPORT" in u) or ("BACK HOME" in u) or ("ARRIVES" in u and "AIRPORT" in u):
        return "RE"
    if "RETOUR" in u:
        return "RE"
    # departure patterns
    if ("NEEDS TO BE AT" in u and "AIRPORT" in u) or ("BE AT" in u and "AIRPORT" in u) or ("VERS" in u and ("BRU" in u or "CRL" in u or "LUX" in u or "ZAV" in u)):
        return "DE"
    # default
    return "DE"

def _extract_name(block_text: str) -> str:
    # Try explicit passenger/traveler labels
    m = re.search(r"(Voyageur\(s\)|Voyageur|Voyageur\(s\)\s*:|Voyageur\s*:)\s*[:\-]?\s*([A-Za-zÀ-ÿ' -]+)", block_text, re.IGNORECASE)
    if m:
        return m.group(2).strip()
    m = re.search(r"(Mr\.?|M\.|Mme|Madame)\s+([A-Za-zÀ-ÿ' -]+)", block_text)
    if m:
        return (m.group(2) or "").strip()
    # Corporate list: "WILMOTTE Denis"
    m = re.search(r"\b([A-ZÀ-ÿ][A-Za-zÀ-ÿ' -]{2,})\s+([A-ZÀ-ÿ][A-Za-zÀ-ÿ' -]{2,})\b", block_text)
    if m:
        # avoid matching common words
        cand = (m.group(1) + " " + m.group(2)).strip()
        return cand
    return ""

def _extract_bdc(block_text: str) -> str:
    # Imputation / CC / BDC
    m = re.search(r"\b(Imputation|BDC|CC)\s*[: ]\s*([A-Za-z0-9\-\/]+)\b", block_text, re.IGNORECASE)
    if m:
        return m.group(2).strip()
    # Example "CC 4219920101"
    m2 = re.search(r"\bCC\s*([0-9]{6,})\b", block_text, re.IGNORECASE)
    if m2:
        return m2.group(1).strip()
    return ""

def _extract_pickup_addr(block_text: str) -> str:
    # EN: pick up in Liege, 47 Boulevard d'Avroy
    m = re.search(r"pick up in\s+[^,]+,\s*([^—\-\n]+)", block_text, re.IGNORECASE)
    if m:
        return m.group(1).strip().rstrip("?")
    # FR: Rue..., Boulevard..., Av..., Route...
    for ln in [l.strip() for l in block_text.split("\n") if l.strip()]:
        u = _norm(ln)
        if any(k in u for k in ["RUE ", "BOULEVARD", "BD ", "AVENUE", "AV ", "ROUTE", "CHAUSS", "PLACE "]):
            if re.search(r"\d", ln):
                return ln.strip()
    return ""

def _extract_dest_addr(block_text: str) -> str:
    # FR labels: "Transfert à :" followed by lines
    m = re.search(r"Transfert\s*à\s*:\s*(.+)", block_text, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    # EN return: "back home" and earlier "pick up in Liege, ..."
    return ""

def _extract_pickup_time(block_text: str) -> str:
    # EN explicit pickup at
    m = re.search(r"pick up at\s+(\d{1,2}[:hH]\d{2})", block_text, re.IGNORECASE)
    if m:
        return _normalize_time_any(m.group(1))
    # FR label Heure / Heure de pick up
    m2 = re.search(r"Heure(?:\s+de\s+pick\s*up)?\s*:\s*(.+)", block_text, re.IGNORECASE)
    if m2:
        h = _normalize_time_any(m2.group(1))
        if h:
            return h
    # phrase "prise en charge à 22h56"
    m3 = re.search(r"prise en charge.*?\b(\d{1,2}\s*[:hH]\s*\d{2})\b", block_text, re.IGNORECASE)
    if m3:
        return _normalize_time_any(m3.group(1))
    # first time in block
    for ln in block_text.split("\n"):
        h = _normalize_time_any(ln)
        if h:
            return h
    return ""

# -----------------------------
# Universal parser
# -----------------------------
_DATE_TOKEN_RE = re.compile(
    r"(?P<d>\d{1,2}[\.\/\-]\d{1,2}[\.\/\-]\d{2,4})|"
    r"(?P<d2>\b(?:Lu|Ma|Me|Je|Ve|Sa|Di)\s+\d{1,2}\/\d{1,2}\b)|"
    r"(?P<d3>\b(?:lundi|mardi|mercredi|jeudi|vendredi|samedi|dimanche)\s+\d{1,2}\/\d{1,2}\b)|"
    r"(?P<d4>\bDate\s*:\s*\d{1,2}\/\d{1,2}(?:\/\d{2,4})?\b)|"
    r"(?P<d5>\b\d{1,2}\s+[A-Za-zÉÈÊËÀÂÄÔÖÛÜÙÇéèêëàâäôöûüùç]+\s+\d{2,4}\b)",
    re.IGNORECASE
)

def split_into_date_blocks(text: str) -> list:
    raw = str(text or "").replace("\r", "\n")
    raw = re.sub(r"[ \t]+", " ", raw)
    lines = [l.rstrip() for l in raw.split("\n")]
    # Build a sequence of (idx, token_text)
    markers = []
    for i, ln in enumerate(lines):
        m = _DATE_TOKEN_RE.search(ln)
        if m:
            markers.append((i, m.group(0).strip()))
    if not markers:
        return []

    blocks = []
    for k, (i, tok) in enumerate(markers):
        start = i
        end = markers[k + 1][0] if k + 1 < len(markers) else len(lines)
        blk_lines = [l.strip() for l in lines[start:end] if l.strip()]
        blocks.append({"date_token": tok, "text": "\n".join(blk_lines)})
    return blocks

def parse_mail_to_navette_universal(text: str, default_year: int = 2026) -> dict:
    if not text or not str(text).strip():
        return {}

    # Special case: BT Tours / CHECK blocks
    u = _norm(text)
    if "CHECK " in u:
        # Let the app's existing CHECK parser handle it (it will call through universal blocks too),
        # but we still allow universal parsing if CHECK parser isn't present.
        pass

    blocks = split_into_date_blocks(text)
    if not blocks:
        # if no date blocks, nothing deterministic -> return {}
        return {}

    transfers = []
    for b in blocks:
        bt = b["text"]
        date_iso = _normalize_date_to_iso(b["date_token"], default_year=default_year) or _normalize_date_to_iso(bt, default_year=default_year)
        if not date_iso:
            continue

        sens = _detect_sens(bt)
        designation, _ = _detect_airport_and_place(bt)
        phone = _extract_phone(bt)
        flight = _extract_flight(bt)
        heure = _extract_pickup_time(bt)

        name = _extract_name(bt)
        bdc = _extract_bdc(bt)

        pickup_addr = _extract_pickup_addr(bt)
        dest_addr = _extract_dest_addr(bt)

        # If it's a return, pickup is airport, dest is home
        if sens == "RE":
            # destination should be address; if missing, reuse pickup_addr found elsewhere in email
            dest_full = dest_addr or pickup_addr
            adr, cp, loc = _split_addr_cp_city(dest_full)
            transfers.append({
                "DATE": date_iso,
                "HEURE": heure,
                "SENS": "RE",
                "DESIGNATION": designation or "BRU",
                "NOM": name,
                "Tél": phone,
                "ADRESSE": adr,
                "CP": cp,
                "Localité": loc,
                "VOL": flight,
                "Num BDC": bdc,
                "PAIEMENT": "Facture",
                "REMARQUE": "",
                "RAW": bt,
            })
        else:
            # departure: pickup is home address if any; designation is airport
            adr, cp, loc = _split_addr_cp_city(pickup_addr)
            transfers.append({
                "DATE": date_iso,
                "HEURE": heure,
                "SENS": "DE",
                "DESIGNATION": designation or "",
                "NOM": name,
                "Tél": phone,
                "ADRESSE": adr,
                "CP": cp,
                "Localité": loc,
                "VOL": flight,
                "Num BDC": bdc,
                "PAIEMENT": "Facture",
                "REMARQUE": "",
                "RAW": bt,
            })

    if not transfers:
        return {}
    return {"MODE": "MAIL", "TRANSFERS": transfers}
