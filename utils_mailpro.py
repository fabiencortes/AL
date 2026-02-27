
import re

def _normalize_french_date_to_ddmmyyyy(s: str) -> str:
    """Convertit '12 mars 26' / '12 mars 2026' / '12/03/26' en '12/03/2026'."""
    s0 = (s or "").strip()
    if not s0:
        return ""

    m = re.search(r"(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{2,4})", s0)
    if m:
        d, mo, y = m.groups()
        y = int(y)
        if y < 100:
            y += 2000
        return f"{int(d):02d}/{int(mo):02d}/{y:04d}"

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

    m2 = re.search(r"(\d{1,2})\s+([A-Za-zÉÈÊËÀÂÄÔÖÛÜÙÇéèêëàâäôöûüùç]+)\s+(\d{2,4})", s0, re.IGNORECASE)
    if m2:
        d, mon, y = m2.groups()
        mon_u = mon.strip().upper()
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
            return f"{int(d):02d}/{int(mo):02d}/{y:04d}"

    return ""


def parse_mail_to_navette_labels(text: str) -> dict:
    """
    Parse les mails entreprise à labels (SECTEUR, Demandeur, Voyageur(s), GSM, Imputation, Date, Heure de pick up, Lieu, Transfert à).
    Retour: {"MODE":"MAIL","TRANSFERS":[{DATE,HEURE,NOM,Tél,ADRESSE,DESIGNATION,Num BDC,REMARQUE,PICKUP,DEST}]}.
    """
    if not text or not str(text).strip():
        return {}

    raw = str(text).replace("\r", "\n")
    raw = re.sub(r"[ \t]+", " ", raw)
    lines = [l.strip() for l in raw.split("\n") if l.strip()]

    def grab(label: str) -> str:
        for i, ln in enumerate(lines):
            if ln.upper().startswith(label.upper()):
                parts = ln.split(":", 1)
                if len(parts) == 2 and parts[1].strip():
                    return parts[1].strip()
                if i + 1 < len(lines):
                    return lines[i + 1].strip()
        return ""

    secteur = grab("SECTEUR")
    sbu = grab("S.B.U.") or grab("SBU")
    demandeur = grab("Demandeur")
    voyageur = grab("Voyageur(s)") or grab("Voyageur")
    gsm = grab("N° GSM du voyageur") or grab("N° GSM") or grab("GSM")
    bdc = grab("Imputation") or grab("Num BDC") or grab("BDC")

    date_txt = grab("Date")
    date_norm = _normalize_french_date_to_ddmmyyyy(date_txt)

    # Heure : chercher après le label HEURE, sinon première heure dans le texte
    heure_txt = grab("Heure de pick up") or grab("Heure") or ""
    m_h = re.search(r"\b(\d{1,2})\s*[:hH]\s*(\d{2})\b", heure_txt)
    if not m_h:
        for ln in lines:
            m_h = re.search(r"\b(\d{1,2})\s*[:hH]\s*(\d{2})\b", ln)
            if m_h:
                break
    heure = ""
    if m_h:
        hh, mm = m_h.groups()
        heure = f"{int(hh):02d}:{int(mm):02d}"

    pickup = grab("Lieu de pick up") or grab("Lieu de pickup") or grab("Lieu") or ""
    # Destination multi-lignes
    dest = ""
    for i, ln in enumerate(lines):
        if ln.upper().startswith("TRANSFERT"):
            chunks = []
            for j in range(i + 1, min(i + 8, len(lines))):
                # stop si on tombe sur un autre label (xxx :)
                if re.match(r"^[A-ZÉÈÊËÀÂÄÔÖÛÜÙÇ0-9 ()'\"./-]+\s*:\s*$", lines[j]):
                    break
                chunks.append(lines[j])
            dest = " ".join(chunks).strip()
            break

    # Si pickup/dest vides, essayer variantes
    if not pickup:
        pickup = grab("Pickup")
    if not dest:
        dest = grab("Destination")

    # DESIGNATION : compacte "PICKUP -> DEST"
    designation = ""
    if pickup and dest:
        designation = f"{pickup} → {dest}"
    elif dest:
        designation = dest
    elif pickup:
        designation = pickup

    remarque_parts = []
    if secteur:
        remarque_parts.append(f"SECTEUR: {secteur}")
    if sbu:
        remarque_parts.append(f"SBU: {sbu}")
    if demandeur:
        remarque_parts.append(f"Demandeur: {demandeur}")
    if bdc:
        remarque_parts.append(f"Imputation: {bdc}")
    remarque = " | ".join(remarque_parts).strip()

    transfer = {
        "DATE": date_norm,
        "HEURE": heure,
        "NOM": voyageur,
        "Tél": gsm,
        "ADRESSE": dest,
        "DEST": dest,
        "PICKUP": pickup,
        "DESIGNATION": designation,
        "Num BDC": bdc,
        "REMARQUE": remarque,
        "RAW": text,
    }

    # Nettoyage clés vides (mais on garde RAW)
    for k in list(transfer.keys()):
        if k == "RAW":
            continue
        if transfer[k] is None:
            transfer[k] = ""
        else:
            transfer[k] = str(transfer[k]).strip()

    # si rien de significatif, abandon
    if not (transfer.get("DATE") or transfer.get("HEURE") or transfer.get("NOM") or transfer.get("ADRESSE") or transfer.get("Tél")):
        return {}

    return {"MODE": "MAIL", "TRANSFERS": [transfer]}
