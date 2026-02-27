
import re

# ------------------------
# Helpers
# ------------------------
def _norm(s: str) -> str:
    return str(s or "").strip().upper()

def _normalize_date_any_to_iso(s: str) -> str:
    """
    Support:
      - 05.03.2026
      - 05/03/2026
      - 05/03/26
      - 5 mars 26 (FR)
    Return ISO YYYY-MM-DD or "".
    """
    s0 = str(s or "").strip()
    if not s0:
        return ""

    # dd.mm.yyyy or dd/mm/yy
    m = re.search(r"(\d{1,2})[\.\/\-](\d{1,2})[\.\/\-](\d{2,4})", s0)
    if m:
        d, mo, y = m.groups()
        y = int(y)
        if y < 100:
            y += 2000
        try:
            return f"{y:04d}-{int(mo):02d}-{int(d):02d}"
        except Exception:
            return ""

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
    m2 = re.search(r"(\d{1,2})\s+([A-Za-zÉÈÊËÀÂÄÔÖÛÜÙÇéèêëàâäôöûüùç]+)\s+(\d{2,4})", s0, re.IGNORECASE)
    if m2:
        d, mon, y = m2.groups()
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

def _normalize_time_any(s: str) -> str:
    """
    Support: 17H15 / 08h15 / 22h56 / 16:55
    Return HH:MM or "".
    """
    s0 = str(s or "")
    m = re.search(r"\b(\d{1,2})\s*[:hH]\s*(\d{2})\b", s0)
    if m:
        hh, mm = m.groups()
        return f"{int(hh):02d}:{int(mm):02d}"
    return ""

def _clean_addr_prefix(s: str) -> str:
    t = str(s or "").strip()
    for pref in ["DOMICILE :", "DOMICILE:", "Domicile :", "Domicile:", "Adresse :", "Adresse:"]:
        if t.startswith(pref):
            t = t[len(pref):].strip()
    return t

def _split_addr_cp_city(full: str):
    s = _clean_addr_prefix(full)
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return "", "", ""
    m = re.search(r"\b(\d{4,5})\s+([A-Za-zÀ-ÿ\-\']+(?:\s+[A-Za-zÀ-ÿ\-\']+)*)$", s)
    if m:
        cp = m.group(1).strip()
        city = m.group(2).strip()
        adr = s[:m.start()].strip(" ,")
        return adr, cp, city
    return s, "", ""

def _extract_client_code(text: str) -> str:
    """
    Ex: 'KNAUF = KI' / 'LEONARDO =LBE' / 'FN HERSTAL = FNH'
    """
    t = str(text or "")
    m = re.search(r"=\s*([A-Z]{2,5})\b", t)
    if m:
        return m.group(1).strip().upper()
    return ""

# ------------------------
# Parsers
# ------------------------

def parse_mail_to_navette_labels(text: str) -> dict:
    """
    Parser labels FR (SECTEUR / Date / Heure / Lieu / Transfert à / etc.)
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

    date_iso = _normalize_date_any_to_iso(grab("Date"))
    heure = _normalize_time_any(grab("Heure de pick up") or grab("Heure") or "")
    if not heure:
        # phrase "prise en charge à 22h56"
        for ln in lines:
            h = _normalize_time_any(ln)
            if h:
                heure = h
                break

    pickup = grab("Lieu de pick up") or grab("Lieu de pickup") or ""
    dest = ""
    for i, ln in enumerate(lines):
        if ln.upper().startswith("TRANSFERT"):
            chunks = []
            for j in range(i + 1, min(i + 10, len(lines))):
                if re.match(r"^[A-ZÉÈÊËÀÂÄÔÖÛÜÙÇ0-9 ()'\"./-]+\s*:\s*$", lines[j]):
                    break
                chunks.append(lines[j])
            dest = " ".join(chunks).strip()
            break

    adr, cp, loc = _split_addr_cp_city(dest)

    transfer = {
        "DATE": date_iso,
        "HEURE": heure,
        "NOM": voyageur,
        "Tél": gsm,
        "ADRESSE": adr,
        "CP": cp,
        "Localité": loc,
        "PICKUP": pickup,
        "DEST": dest,
        "Num BDC": bdc,
        "Demandeur": demandeur,
        "SECTEUR": secteur,
        "SBU": sbu,
        "RAW": text,
    }
    # if nothing meaningful, abort
    if not (transfer["DATE"] or transfer["HEURE"] or transfer["NOM"] or transfer["Tél"] or transfer["ADRESSE"]):
        return {}
    return {"MODE": "MAIL", "TRANSFERS": [transfer]}


def parse_mail_knauf_english(text: str) -> dict:
    """
    Exemple KNAUF (EN) multi-dates.
    Pattern:
      05.03.2026 ... Brussels airport ... flight leaves at 16:55 (LO238) ... pick up in Liege, 47 Boulevard ... pick up at 13:50?
      06.03.2026 ... from Brussels airport back home ... arrives –18:45 ( LO233)
    """
    if not text or not str(text).strip():
        return {}
    t = str(text).replace("\r", "\n")
    client_code = _extract_client_code(t)  # KI
    # mobile
    gsm = ""
    m_g = re.search(r"Mobile\s+from\s+.*?:\s*([+0-9][0-9 \-]+)", t, re.IGNORECASE)
    if m_g:
        gsm = m_g.group(1).strip()
    # cost center / BDC
    bdc = ""
    m_cc = re.search(r"\bCC\s*([0-9]{6,})\b", t, re.IGNORECASE)
    if m_cc:
        bdc = m_cc.group(1).strip()

    # traveller name
    name = ""
    m_nm = re.search(r"for\s+Mr\.?\s*([A-Za-zÀ-ÿ' -]+)\s*:", t, re.IGNORECASE)
    if m_nm:
        name = m_nm.group(1).strip()

    lines = [l.strip() for l in t.split("\n") if l.strip()]
    transfers = []

    # Each date line starts with dd.mm.yyyy
    i = 0
    while i < len(lines):
        ln = lines[i]
        m_date = re.match(r"^(\d{1,2}[\.\/]\d{1,2}[\.\/]\d{2,4})\b(.*)$", ln)
        if not m_date:
            i += 1
            continue

        date_iso = _normalize_date_any_to_iso(m_date.group(1))
        chunk = ln
        # also consume following non-date lines until next date
        j = i + 1
        while j < len(lines) and not re.match(r"^\d{1,2}[\.\/]\d{1,2}[\.\/]\d{2,4}\b", lines[j]):
            chunk += " " + lines[j]
            j += 1

        # flight and airport
        flight = ""
        m_fl = re.search(r"\b([A-Z]{1,2}\d{2,5})\b", chunk)
        if m_fl:
            flight = m_fl.group(1).strip().upper()

        # time: prefer pickup at HH:MM / HHhMM, else arrives/leaves
        heure = ""
        m_pu = re.search(r"pick up at\s+(\d{1,2}[:hH]\d{2})", chunk, re.IGNORECASE)
        if m_pu:
            heure = _normalize_time_any(m_pu.group(1))
        if not heure:
            m_arr = re.search(r"arrives\s*[–\-]?\s*(\d{1,2}[:hH]\d{2})", chunk, re.IGNORECASE)
            if m_arr:
                heure = _normalize_time_any(m_arr.group(1))
        if not heure:
            m_lv = re.search(r"leaves\s+at\s+(\d{1,2}[:hH]\d{2})", chunk, re.IGNORECASE)
            if m_lv:
                # for departure legs, pickup is earlier; but we keep leaves time if pickup missing
                heure = _normalize_time_any(m_lv.group(1))

        # Determine sens and pickup/dest
        # If "be at Brussels airport" => home -> BRU (DE)
        # If "from Brussels airport back home" => BRU -> home (RE)
        u = _norm(chunk)
        is_return = ("FROM BRUSSELS" in u and "BACK HOME" in u) or ("FROM DUESSELDORF" in u and "BACK HOME" in u)
        # airport keyword
        airport = ""
        if "BRUSSELS" in u:
            airport = "BRU"
        elif "DUESSELDORF" in u or "DUSSELDORF" in u or "DÜSSELDORF" in u:
            airport = "DUS"
        elif "LUXEMBOURG" in u:
            airport = "LUX"

        # address
        adr = ""
        m_addr = re.search(r"pick up in\s+([^,]+),\s*([^—\-]+?)(?:\s*-\s*Mr|\s*$)", chunk, re.IGNORECASE)
        if m_addr:
            # city, address
            adr = (m_addr.group(2) or "").strip()
        # fallback: "back home" no address in chunk => keep empty (user may fill)
        adr, cp, loc = _split_addr_cp_city(adr)

        if is_return:
            sens = "RE"
            designation = airport or ""
            pickup = airport or ""
            dest = adr or "HOME"
        else:
            sens = "DE"
            designation = airport or ""
            pickup = adr or "HOME"
            dest = airport or ""

        transfers.append({
            "DATE": date_iso,
            "HEURE": heure,
            "SENS": sens,
            "DESIGNATION": designation,
            "NOM": name,
            "Tél": gsm,
            "ADRESSE": adr,
            "CP": cp,
            "Localité": loc,
            "VOL": flight,
            "Num BDC": bdc,
            
            "RAW": chunk
        })

        i = j

    if not transfers:
        return {}
    return {"MODE": "MAIL", "TRANSFERS": transfers}


def parse_mail_leonardo_fr(text: str) -> dict:
    """
    Exemple LEONARDO (FR) :
      Date : lundi 02/03
      De : BRU
      Vers : VDV Sélys
      Heure : 17H15 (A3488)
    Multi-blocs.
    """
    if not text or not str(text).strip():
        return {}
    t = str(text).replace("\r", "\n")
    client_code = _extract_client_code(t)  # LBE
    lines = [l.strip() for l in t.split("\n") if l.strip()]

    # pax / contact
    pax = ""
    gsm = ""
    m_pax = re.search(r"\b(\d+)\s*PAX\b", t, re.IGNORECASE)
    if m_pax:
        pax = m_pax.group(1).strip()
    m_g = re.search(r"\+?\d[\d \-]{7,}", t)
    if m_g:
        gsm = m_g.group(0).strip()

    transfers = []
    i = 0
    while i < len(lines):
        if lines[i].upper().startswith("DATE"):
            date_txt = lines[i].split(":", 1)[-1].strip() if ":" in lines[i] else ""
            # sometimes next token is "lundi 02/03"
            if not date_txt and i + 1 < len(lines):
                date_txt = lines[i+1]
            # year may be missing => assume 2026 if not present (your planning year)
            date_iso = _normalize_date_any_to_iso(date_txt)
            if not date_iso:
                m = re.search(r"(\d{1,2})[\/\-](\d{1,2})", date_txt)
                if m:
                    d, mo = m.groups()
                    date_iso = f"2026-{int(mo):02d}-{int(d):02d}"

            de = ""
            vers = ""
            heure = ""
            vol = ""

            # scan next ~6 lines
            for j in range(i+1, min(i+10, len(lines))):
                lj = lines[j]
                if lj.upper().startswith("DATE"):
                    break
                if lj.upper().startswith("DE"):
                    de = lj.split(":",1)[-1].strip()
                elif lj.upper().startswith("VERS"):
                    vers = lj.split(":",1)[-1].strip()
                elif lj.upper().startswith("HEURE"):
                    heure = _normalize_time_any(lj)
                    mfl = re.search(r"\b([A-Z]{1,2}\d{2,5})\b", lj)
                    if mfl:
                        vol = mfl.group(1).upper()
            # sens: if de is airport code => RE? actually "De: BRU Vers: hotel" is RE (from airport to hotel)
            u_de = _norm(de)
            sens = "RE" if u_de in ("BRU","CRL","LUX","ZAV","DUS","MIDI") else "DE"
            designation = u_de if u_de else _norm(vers)
            transfers.append({
                "DATE": date_iso,
                "HEURE": heure,
                "SENS": sens,
                "DESIGNATION": designation,
                "PAX": pax,
                "Tél": gsm,
                "VOL": vol,
                "DE": de,
                "VERS": vers,
                
                "RAW": "\n".join(lines[i:min(i+12,len(lines))])
            })
        i += 1

    if not transfers:
        return {}
    return {"MODE":"MAIL","TRANSFERS":transfers}


def parse_mail_fnherstal(text: str) -> dict:
    """
    Exemple FN HERSTAL :
      Sa 21/03
      Devra être à ZAV pour 11H00 AM
      Rue ... NANDRIN
      GSM ...
      Ve 27/03
      ZAV 07H05 TG934 ex Bangkok à NANDRIN
    """
    if not text or not str(text).strip():
        return {}
    t = str(text).replace("\r","\n")
    client_code = _extract_client_code(t)  # FNH
    lines = [l.strip() for l in t.split("\n") if l.strip()]
    gsm = ""
    m_g = re.search(r"\b0\d{2,3}\s*\d{2,3}\s*\d{2}\s*\d{2}\b", t)
    if m_g:
        gsm = re.sub(r"\s+","",m_g.group(0))

    transfers = []
    # detect date markers like "Sa 21/03" / "Ve 27/03"
    for i, ln in enumerate(lines):
        m = re.match(r"^(Lu|Ma|Me|Je|Ve|Sa|Di)\s+(\d{1,2})\/(\d{1,2})\b", ln, re.IGNORECASE)
        if not m:
            continue
        d = int(m.group(2)); mo = int(m.group(3))
        date_iso = f"2026-{mo:02d}-{d:02d}"

        # gather following lines until next date marker
        chunk = []
        j = i+1
        while j < len(lines) and not re.match(r"^(Lu|Ma|Me|Je|Ve|Sa|Di)\s+\d{1,2}\/\d{1,2}\b", lines[j], re.IGNORECASE):
            chunk.append(lines[j]); j += 1
        block = " ".join(chunk)
        heure = _normalize_time_any(block)

        # airport code appears as ZAV
        u = _norm(block)
        designation = "ZAV" if "ZAV" in u else ""
        # address (first line with street number)
        addr = ""
        for k in chunk:
            if re.search(r"\d+\s*[-–]?\s*\d*", k) and ("RUE" in _norm(k) or "AV" in _norm(k) or "BD" in _norm(k)):
                addr = k.strip()
                break
        adr, cp, loc = _split_addr_cp_city(addr)
        sens = "DE" if "DEVRA ETRE" in u or "DEVRA ÊTRE" in u else "RE" if u.startswith("ZAV") else ""
        transfers.append({
            "DATE": date_iso,
            "HEURE": heure,
            "SENS": sens or "DE",
            "DESIGNATION": designation,
            "Tél": gsm,
            "ADRESSE": adr,
            "CP": cp,
            "Localité": loc or "",
            
            "RAW": "\n".join([ln] + chunk)
        })

    if not transfers:
        return {}
    return {"MODE":"MAIL","TRANSFERS":transfers}




def parse_airportlines_check_doc(text: str) -> dict:
    """
    Format type 'AIRPORT LINES' doc (Srelax) :
      23/03/2026
      AIRPORT LINES
      CHECK BRU 08H15
      117501/5 ... 1 MR HANS MICHEL ...
      07H30  RUE DE HUY 92 - 4300 WAREMME  0478 69 51 31
      117501/5 ... 2 MRS PIRSON JOCELYNE ...
      BRU 10H20 SK594

    On crée une navette DE vers l'aéroport (DESIGNATION=BRU/ZYR/...) avec HEURE=heure pickup,
    VOL=code vol si présent, ADRESSE=adresse pickup, Tél=tel si présent.
    """
    if not text or not str(text).strip():
        return {}
    t = str(text).replace("\r", "\n")
    lines = [l.strip() for l in t.split("\n") if l.strip()]

    # date en tête
    date_iso = ""
    if lines:
        m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", lines[0])
        if m:
            d, mo, y = m.groups()
            date_iso = f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"

    transfers = []

    # Split by CHECK lines
    i = 0
    while i < len(lines):
        ln = lines[i]
        m_check = re.match(r"^CHECK\s+([A-Z]{3})\s+(\d{1,2})H(\d{2})$", _norm(ln))
        if not m_check:
            i += 1
            continue
        airport = m_check.group(1).strip().upper()
        check_time = f"{int(m_check.group(2)):02d}:{int(m_check.group(3)):02d}"

        # collect until next CHECK or end
        j = i + 1
        seg = []
        while j < len(lines) and not _norm(lines[j]).startswith("CHECK "):
            seg.append(lines[j])
            j += 1

        # passenger lines: booking ... (MR/MRS/MS) NAME ...
        pax_list = []
        for s in seg:
            m_p = re.search(r"\b(MR|MRS|MS|MME|M\\.|MADAME|MONSIEUR)\b\s+([A-ZÀ-ÿ' -]+)", s.upper())
            if m_p:
                title = m_p.group(1)
                name = m_p.group(2).strip()
                # avoid capturing too much trailing product text: keep first 3 words max after title if long
                name = " ".join(name.split()[:4])
                # pax count if exists like "... F   2 MR ..."
                pax_n = ""
                m_n = re.search(r"\bF\s+(\d)\b", s.upper())
                if m_n:
                    pax_n = m_n.group(1)
                pax_list.append((name.title(), pax_n))

        # pickup line: starts with HHMM and contains '-' and a phone
        pickup_time = ""
        pickup_addr = ""
        tel = ""
        vol = ""
        flight_time = ""

        for s in seg:
            # pickup time/address
            m_pick = re.match(r"^(\d{1,2})H(\d{2})\s+(.*)$", s.upper())
            if m_pick and ("RUE" in m_pick.group(3) or "-" in m_pick.group(3)):
                pickup_time = f"{int(m_pick.group(1)):02d}:{int(m_pick.group(2)):02d}"
                rest = s.strip()
                # phone
                m_tel = re.search(r"(\+?\d[\d\s]{7,}\d)", rest)
                if m_tel:
                    tel = re.sub(r"\s+", "", m_tel.group(1))
                    pickup_addr = rest.replace(m_tel.group(0), "").strip()
                else:
                    pickup_addr = rest.strip()

            # flight line: "BRU 10H20 SK594" or "ZYR 10H06 AF7184"
            m_f = re.match(r"^([A-Z]{3})\s+(\d{1,2})H(\d{2})\s+([A-Z]{1,3}\d{2,5})$", s.upper())
            if m_f:
                vol = m_f.group(4).strip().upper()
                flight_time = f"{int(m_f.group(2)):02d}:{int(m_f.group(3)):02d}"

        # Defaults
        if not pickup_time:
            pickup_time = check_time  # fallback

        adr, cp, loc = _split_addr_cp_city(pickup_addr)

        if not pax_list:
            pax_list = [("", "")]

        for (nm, pax_n) in pax_list:
            transfers.append({
                "DATE": date_iso,
                "HEURE": pickup_time,
                "SENS": "DE",
                "DESIGNATION": airport,
                "NOM": nm,
                "PAX": pax_n,
                "Tél": tel,
                "ADRESSE": adr,
                "CP": cp,
                "Localité": loc,
                "VOL": vol,
                "REMARQUE": (f"CHECK {airport} {check_time}" + (f" | FLIGHT {flight_time}" if flight_time else "")),
                "RAW": "\n".join([ln] + seg),
            })

        i = j

    if not transfers:
        return {}
    return {"MODE": "MAIL", "TRANSFERS": transfers}


def parse_mail_to_navette_any(text: str) -> dict:
    """
    Dispatcher: essaye plusieurs formats, retourne {"MODE":"MAIL","TRANSFERS":[...]}
    """
    # 1) labels FR (John Cockerill-like)
    d = parse_mail_to_navette_labels(text)
    if d:
        return d
    # 2) Airport Lines CHECK (doc/pdf)
    d = parse_airportlines_check_doc(text)
    if d:
        return d
    # 3) Knauf EN
    d = parse_mail_knauf_english(text)
    if d:
        return d
    # 3) Leonardo FR
    d = parse_mail_leonardo_fr(text)
    if d:
        return d
    # 4) FN Herstal
    d = parse_mail_fnherstal(text)
    if d:
        return d
    return {}
