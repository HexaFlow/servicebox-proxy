#!/usr/bin/env python3
"""
ServiceBox Local Proxy
Runs on the garage PC (on Stellantis VPN) to bridge the frontend
with ServiceBox + Alpha DMS.
"""

import base64
import ctypes
import os
import re
import json
import random
import sqlite3
import subprocess
import sys
import time
import threading
import traceback
from collections import deque
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Optional, List

import requests
import urllib3
from requests_ntlm import HttpNtlmAuth
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from version import VERSION

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BACKEND_URL = "https://api-727559206231.europe-west9.run.app"

app = FastAPI(title="ServiceBox Proxy", version=VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── In-memory log ring buffer ────────────────────────────────────────────

_log_entries: deque = deque(maxlen=2000)


def _log(level: str, message: str, step: str = ""):
    entry = {
        "ts": datetime.now().isoformat(),
        "level": level,
        "step": step,
        "message": message,
    }
    _log_entries.append(entry)
    prefix = f"[{step}] " if step else ""
    print(f"[{level.upper()}] {prefix}{message}")


# ─── Operations database (SQLite) ───────────────────────────────────────

def _ops_db_path() -> str:
    """Database file sits next to the executable (or script)."""
    if getattr(sys, "frozen", False):
        base = os.path.dirname(sys.executable)
    else:
        base = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base, "operations.db")


def _ops_init():
    """Create the operations table if it doesn't exist."""
    con = sqlite3.connect(_ops_db_path())
    con.execute("""
        CREATE TABLE IF NOT EXISTS operations (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp   TEXT    NOT NULL,
            operation   TEXT    NOT NULL,
            username    TEXT    NOT NULL DEFAULT '',
            request_data TEXT   NOT NULL DEFAULT '{}',
            response_data TEXT  NOT NULL DEFAULT '{}',
            success     INTEGER NOT NULL DEFAULT 1,
            error       TEXT    NOT NULL DEFAULT '',
            duration_ms INTEGER NOT NULL DEFAULT 0
        )
    """)
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_ops_timestamp ON operations(timestamp)
    """)
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_ops_operation ON operations(operation)
    """)
    con.commit()
    con.close()


_ops_init()


def _ops_record(
    operation: str,
    username: str = "",
    request_data: dict | None = None,
    response_data: dict | None = None,
    success: bool = True,
    error: str = "",
    duration_ms: int = 0,
):
    """Insert one operation record."""
    try:
        con = sqlite3.connect(_ops_db_path())
        con.execute(
            """INSERT INTO operations
               (timestamp, operation, username, request_data, response_data, success, error, duration_ms)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                datetime.now(timezone.utc).isoformat(),
                operation,
                username,
                json.dumps(request_data or {}, default=str),
                json.dumps(response_data or {}, default=str),
                1 if success else 0,
                error,
                duration_ms,
            ),
        )
        con.commit()
        con.close()
    except Exception as e:
        _log("error", f"Echec enregistrement operation: {e}", "ops")


def _ops_safe_request(req, exclude_fields: set | None = None) -> dict:
    """Extract loggable fields from a Pydantic request, excluding credentials."""
    exclude = {"username", "password", "site_code"} | (exclude_fields or set())
    return {k: v for k, v in req.dict().items() if k not in exclude}


# ─── In-memory session cache (keyed by username) ─────────────────────────

_sessions: dict[str, "ServiceBoxSession"] = {}


# ─── Pydantic models ─────────────────────────────────────────────────────

class Credentials(BaseModel):
    username: str
    password: str
    site_code: Optional[str] = None


class AgendaOptionsRequest(Credentials):
    pass


class CreateRdvRequest(Credentials):
    # Planning
    date: str          # YYYYMMDD
    heure: str         # HHMM
    restitution_heure: str  # HHMM
    receptionnaire_id: str
    equipe_id: str
    # Client
    nom: str
    prenom: str
    tel_mobile: str
    email: str = ""
    civilite_id: str = "1"          # 1=M., 2=Mme
    indicatif_tel: str = "33"
    # Vehicle
    marque: str = "AC"              # AC = Peugeot/Citroen brand code
    ldp_libelle: str = "PEUGEOT"
    marque_libelle: str = ""
    vin: str = ""
    immatriculation: str = ""
    kilometrage: str = ""
    mec_day: str = ""
    mec_month: str = ""
    mec_year: str = ""
    # Intervention (array of work items)
    travaux: list[dict] = []  # [{"nom": "...", "duree": "0.50"}, ...]
    # Legacy single-intervention fields (used as fallback if travaux is empty)
    travail_nom: str = ""
    travail_duree: str = "0.50"
    # Config
    pdv_id: str = "011622H"
    code_pays: str = "FR"


class StepResult(BaseModel):
    name: str
    status: str  # "ok" | "error" | "skipped"
    detail: str = ""


class CreateRdvResponse(BaseModel):
    success: bool
    or_number: Optional[str] = None
    dossier_id: Optional[str] = None
    rdv_id: Optional[str] = None
    client_id: Optional[str] = None
    error: Optional[str] = None
    steps: list[StepResult] = []


class AgendaOptionsResponse(BaseModel):
    receptionnaires: list[dict]  # [{id, name}]
    equipes: list[dict]          # [{id, name}]


class FetchEstimationRequest(Credentials):
    dossier_id: str


class FetchEstimationResponse(BaseModel):
    success: bool
    html: Optional[str] = None
    error: Optional[str] = None


class TestConnectionResponse(BaseModel):
    connected: bool
    session_ok: bool
    servicebox_reachable: bool
    detail: str = ""


# ─── ServiceBox Session ──────────────────────────────────────────────────

class ServiceBoxSession:
    def __init__(self, username: str, password: str):
        self.user = username
        self.password = password
        self.session = requests.Session()
        self.session.verify = False  # Corporate VPNs often have SSL inspection
        self.base_url = "https://servicebox.mpsa.com"
        self.auth_method = "unknown"

        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
            "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8",
            "Connection": "keep-alive",
        })

    def bootstrap(self):
        _log("info", f"Bootstrap avec user={self.user}...", "bootstrap")
        login_url = f"{self.base_url}/agenda/planningReceptionnaire.action"

        # Strategy: mimic exact browser behavior
        # 1. Hit URL with session (no auth) — BigIP returns 401 + may set tracking cookies
        # 2. Resend same URL with Basic Auth on same session (cookies preserved)
        # This is exactly what browsers do with Basic Auth.

        _log("info", "Etape 1: requete initiale sans auth (challenge BigIP)...", "bootstrap")
        challenge_resp = self.session.get(login_url, timeout=15, allow_redirects=False)
        www_auth = challenge_resp.headers.get("WWW-Authenticate", "")
        _log("info", f"Challenge: HTTP {challenge_resp.status_code}, WWW-Authenticate: '{www_auth}'", "bootstrap")
        _log("info", f"Challenge cookies: {list(self.session.cookies.keys())}", "bootstrap")
        _log("info", f"Challenge headers: {json.dumps(dict(challenge_resp.headers), default=str)}", "bootstrap")

        # 2. Now send with Basic Auth (session retains any cookies from step 1)
        _log("info", "Etape 2: requete avec Basic Auth...", "bootstrap")
        auth_str = f"{self.user}:{self.password}"
        auth_header = f"Basic {base64.b64encode(auth_str.encode()).decode()}"
        self.session.headers["Authorization"] = auth_header
        self.auth_method = "basic"

        resp = self.session.get(login_url, timeout=30)
        _log("info", f"Bootstrap Basic: HTTP {resp.status_code} ({len(resp.text)} bytes, cookies={list(self.session.cookies.keys())})", "bootstrap")
        _log("info", f"Response headers: {json.dumps(dict(resp.headers), default=str)}", "bootstrap")

        # If Basic fails, try NTLM
        if resp.status_code == 401:
            _log("info", "Basic echoue, tentative NTLM...", "bootstrap")
            self.session.headers.pop("Authorization", None)
            self.session.auth = HttpNtlmAuth(self.user, self.password)
            self.auth_method = "ntlm"
            resp = self.session.get(login_url, timeout=30)
            _log("info", f"Bootstrap NTLM: HTTP {resp.status_code} ({len(resp.text)} bytes, cookies={list(self.session.cookies.keys())})", "bootstrap")

        # If still 401, try with domain prefix variants
        if resp.status_code == 401:
            for domain in ["MPSA", "STELLANTIS", "PSA", "GROUPE-PSA"]:
                _log("info", f"Tentative avec domaine {domain}\\{self.user}...", "bootstrap")
                self.session.headers.pop("Authorization", None)
                self.session.auth = None
                domain_user = f"{domain}\\{self.user}"
                auth_str = f"{domain_user}:{self.password}"
                self.session.headers["Authorization"] = f"Basic {base64.b64encode(auth_str.encode()).decode()}"
                self.auth_method = f"basic+{domain}"
                resp = self.session.get(login_url, timeout=30)
                _log("info", f"Bootstrap {domain}: HTTP {resp.status_code} ({len(resp.text)} bytes)", "bootstrap")
                if resp.status_code != 401:
                    break

        _log("info", f"Auth method final: {self.auth_method}", "bootstrap")

        # After bootstrap, exchange SSO token and sync to backend
        self._sync_sso_token()

    def _sync_sso_token(self):
        """Get SSO token from multibrand API and push it to the backend."""
        try:
            _log("info", "Echange SSO token...", "ssoSync")
            cmm_resp = self.session.get(f"{self.base_url}/docapvpr/$cmm/", timeout=15)
            if cmm_resp.status_code != 200:
                _log("warn", f"$cmm returned {cmm_resp.status_code}", "ssoSync")
                return

            match = re.search(
                r"var appUrl = '(https://multibrand\.servicebox-parts\.com[^']+)'",
                cmm_resp.text,
            )
            if not match:
                _log("warn", "Pas de appUrl dans la reponse $cmm", "ssoSync")
                return

            from urllib.parse import urlparse
            app_url = match.group(1)
            parsed = urlparse(app_url)
            sso_param = parsed.query

            login_resp = requests.get(
                "https://multibrand.servicebox-parts.com/bo/common/v1/api/login",
                params={"p": sso_param},
                timeout=15,
            )
            if login_resp.status_code != 200:
                _log("warn", f"Multibrand login returned {login_resp.status_code}", "ssoSync")
                return

            data = login_resp.json()
            sso_token = data.get("token")
            if not sso_token:
                _log("warn", "Pas de token dans la reponse login", "ssoSync")
                return

            # Decode JWT to get user_id
            import base64 as b64
            payload_b64 = sso_token.split(".")[1]
            payload_b64 = payload_b64.replace("-", "+").replace("_", "/")
            payload_b64 += "=" * (4 - len(payload_b64) % 4)
            payload = json.loads(b64.b64decode(payload_b64).decode())
            user_id = payload.get("sub", "")

            _log("info", f"SSO token obtenu (user_id={user_id})", "ssoSync")

            # Push to backend
            try:
                resp = requests.post(
                    f"{BACKEND_URL}/public/servicebox/sync-token",
                    json={
                        "username": self.user,
                        "password": self.password,
                        "sso_token": sso_token,
                        "user_id": user_id,
                    },
                    timeout=10,
                )
                if resp.status_code == 200:
                    _log("info", "SSO token synchronise avec le backend", "ssoSync")
                else:
                    _log("warn", f"Backend sync returned {resp.status_code}: {resp.text[:200]}", "ssoSync")
            except Exception as e:
                _log("warn", f"Backend sync echoue: {e}", "ssoSync")

        except Exception as e:
            _log("warn", f"SSO token exchange echoue: {e}", "ssoSync")

    def get_agenda_options(self) -> dict:
        url = f"{self.base_url}/agenda/creerRdv.action"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": self.base_url,
            "Referer": f"{self.base_url}/agenda/?tabControlID=&jbnContext=true",
            "Accept": "text/html, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
        }
        resp = self.session.post(url, data={
            "date": datetime.now().strftime("%Y%m%d"),
            "heure": "0900",
            "receptionnaireId": "0",
        }, headers=headers)
        if resp.status_code != 200:
            raise HTTPException(502, f"ServiceBox creerRdv returned {resp.status_code}")

        # Parse receptionnaires
        recep_match = re.search(
            r'<select\s[^>]*id="agentreception"[^>]*>(.*?)</select>',
            resp.text, re.DOTALL,
        )
        receptionnaires = []
        if recep_match:
            receptionnaires = [
                {"id": val, "name": " ".join(name.split())}
                for val, name in re.findall(
                    r'<option\s[^>]*value="([^"]*)"[^>]*>(.*?)</option>',
                    recep_match.group(1), re.DOTALL,
                )
                if val.strip()
            ]

        # Parse equipes
        equipe_match = re.search(
            r'<select\s[^>]*id="ldt-equipe-0"[^>]*>(.*?)</select>',
            resp.text, re.DOTALL,
        )
        equipes = []
        if equipe_match:
            equipes = [
                {"id": val, "name": " ".join(name.split())}
                for val, name in re.findall(
                    r'<option\s[^>]*value="([^"]*)"[^>]*>(.*?)</option>',
                    equipe_match.group(1), re.DOTALL,
                )
                if val.strip()
            ]

        return {"receptionnaires": receptionnaires, "equipes": equipes}

    def _search_client_dms(self, phone: str, nom: str = "") -> dict | None:
        """Search for an existing client via Alpha DMS RelaisServlet.
        Tries phone search first, then falls back to name search.
        Returns {"dms_id": ..., "nom": ..., "prenom": ...} or None.
        """
        if not phone and not nom:
            return None

        clean_phone = re.sub(r"[^0-9]", "", phone) if phone else ""

        _log("info", f"Recherche client DMS par tel: {clean_phone}", "rechercheClient")

        try:
            relais_url = self._get_relais_url()
            if not relais_url:
                _log("warn", "Impossible de trouver l'URL RelaisServlet", "rechercheClient")
                return None
            _log("info", f"RelaisServlet: {relais_url}", "rechercheClient")

            if clean_phone and len(clean_phone) >= 6:
                result = self._dms_search(relais_url, code_interrogation="6", search_value=clean_phone)
                if result:
                    return result

            _log("info", "Aucun client existant trouve, un nouveau sera cree", "rechercheClient")
            return None

        except Exception as e:
            _log("warn", f"Recherche client DMS echouee: {e}", "rechercheClient")
            return None

    def _dms_search(self, relais_url: str, code_interrogation: str, search_value: str) -> dict | None:
        """Execute a single DMS search via RelaisServlet. Returns client dict or None."""
        _log("info", f"DMS search: code={code_interrogation}, value={search_value}", "rechercheClient")

        try:
            dms_xml = (
                f'<DMS TYPE = "01" CODE_INTERROGATION = "{code_interrogation}" '
                f'CHAMPS_CMPL = "{search_value}" '
                f'NumeroPoste = "@P@" '
                f'PARAMDMS = "R:CIT" '
                f'></DMS>'
            )

            from urllib.parse import quote as url_quote
            encoded_xml = url_quote(dms_xml, safe="")

            relais_headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": self.base_url,
                "Accept": "text/plain, */*; q=0.01",
            }
            relais_resp = self.session.post(
                relais_url,
                data={
                    "ajax": "true",
                    "NumeroPoste": "@P@",
                    "PARAMDMS": "R:CIT",
                    "urlResp": "dummy",
                    "xml": encoded_xml,
                },
                headers=relais_headers,
                verify=False,
            )

            if relais_resp.status_code != 200:
                _log("warn", f"RelaisServlet HTTP {relais_resp.status_code}", "rechercheClient")
                return None

            dms_response_html = relais_resp.text
            _log("info", f"Reponse DMS ({len(dms_response_html)} bytes)", "rechercheClient")
            _log("info", f"Raw DMS response: {dms_response_html[:500]}", "rechercheClient")

            # Parse CLIENT elements from the DMS XML response
            xml_match = re.search(r'name="xml"\s+value="([^"]*)"', dms_response_html)
            if not xml_match:
                _log("info", "Pas de champ xml dans la reponse DMS", "rechercheClient")
                return None

            from urllib.parse import unquote_plus
            xml_data = unquote_plus(xml_match.group(1))
            _log("info", f"XML DMS: {xml_data[:500]}", "rechercheClient")

            # Parse CLIENT elements
            clients = re.findall(
                r'<CLIENT\s+([^>]+)/?>',
                xml_data,
            )
            if not clients:
                _log("info", f"Aucun CLIENT dans la reponse DMS (code={code_interrogation}, value={search_value})", "rechercheClient")
                return None

            # Parse first client's attributes
            first_client_attrs = clients[0]
            dms_id = re.search(r'CLIENT_DMS_ID\s*=\s*"([^"]*)"', first_client_attrs)
            nom_match = re.search(r'Nom\s*=\s*"([^"]*)"', first_client_attrs)
            prenom_match = re.search(r'Prenom\s*=\s*"([^"]*)"', first_client_attrs)

            if not dms_id:
                _log("warn", "CLIENT_DMS_ID introuvable", "rechercheClient")
                return None

            client = {
                "dms_id": dms_id.group(1),
                "nom": nom_match.group(1) if nom_match else "",
                "prenom": prenom_match.group(1) if prenom_match else "",
            }
            _log("info", f"Client DMS trouve: id={client['dms_id']}, {client['prenom']} {client['nom']} ({len(clients)} resultats)", "rechercheClient")

            # Select this client in ServiceBox session
            _log("info", "Selection du client dans ServiceBox...", "rechercheClient")
            select_resp = self.session.post(
                f"{self.base_url}/agenda/dmsClientVehiculeSelection.action",
                data={
                    "champId": "tel-2",
                    "dmsReponse": dms_response_html,
                },
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Origin": self.base_url,
                    "Referer": f"{self.base_url}/agenda/?tabControlID=&jbnContext=true",
                    "Accept": "text/html, */*; q=0.01",
                    "X-Requested-With": "XMLHttpRequest",
                },
            )
            _log("info", f"dmsClientVehiculeSelection: HTTP {select_resp.status_code}", "rechercheClient")

            return client

        except Exception as e:
            _log("warn", f"DMS search error: {e}", "rechercheClient")
            return None

    def _get_relais_url(self) -> str | None:
        """Extract the RelaisServlet URL from the agenda page config."""
        # The RelaisServlet URL is typically in a JS variable or hidden field
        # Try fetching the agenda page and parsing it
        try:
            resp = self.session.get(
                f"{self.base_url}/agenda/?tabControlID=&jbnContext=true",
                headers={"Accept": "text/html"},
            )
            if resp.status_code != 200:
                return None
            # Look for RelaisServlet URL pattern
            match = re.search(r'(https?://[^"\']+/eDMS/RelaisServlet)', resp.text)
            if match:
                return match.group(1)
            # Look for dmsUrl or similar config
            match = re.search(r'dmsUrl\s*[=:]\s*["\']([^"\']+)["\']', resp.text)
            if match:
                return match.group(1)
            return None
        except Exception:
            return None

    def delete_rdv(self, rdv_id: str) -> dict:
        """Delete an RDV from ServiceBox agenda."""
        _log("info", f"Suppression RDV {rdv_id}...", "supprimerRdv")
        ts = int(datetime.now(timezone.utc).timestamp() * 1000)
        resp = self.session.get(
            f"{self.base_url}/agenda/supprimerRdv.action",
            params={"rdvId": rdv_id, "_": str(ts)},
            headers={
                "Accept": "*/*",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": f"{self.base_url}/agenda/?tabControlID=&jbnContext=true",
            },
        )
        _log("info", f"HTTP {resp.status_code} ({len(resp.text)} bytes)", "supprimerRdv")
        if resp.status_code != 200:
            _log("error", f"Echec HTTP {resp.status_code}", "supprimerRdv")
            return {"success": False, "error": f"HTTP {resp.status_code}"}
        _log("info", "RDV supprime", "supprimerRdv")
        return {"success": True}

    def create_rdv(self, req: CreateRdvRequest) -> CreateRdvResponse:
        """Create RDV + transfer to Alpha DMS. Returns OR number with step tracking."""
        steps: list[StepResult] = []

        _log("info", f"=== Creation RDV ===", "creerRdv")
        _log("info", f"Date: {req.date}, Heure reception: {req.heure}, Heure restitution: {req.restitution_heure}", "creerRdv")
        _log("info", f"Receptionnaire: {req.receptionnaire_id}, Equipe: {req.equipe_id}", "creerRdv")
        _log("info", f"Client: {req.prenom} {req.nom}, Tel: {req.tel_mobile}", "creerRdv")
        _log("info", f"Vehicule: {req.marque_libelle} {req.ldp_libelle}, Immat: {req.immatriculation}, VIN: {req.vin}", "creerRdv")
        # Resolve interventions: use travaux array if provided, else fall back to legacy fields
        travaux = req.travaux if req.travaux else [{"nom": req.travail_nom or "Intervention", "duree": req.travail_duree}]
        total_duree = f"{sum(float(t.get('duree', 0.5)) for t in travaux):.2f}"
        for t in travaux:
            _log("info", f"Travail: {t['nom']} ({t.get('duree', '0.50')}h)", "creerRdv")

        # Step 0: Search for existing client in Alpha DMS
        existing_client = self._search_client_dms(req.tel_mobile, nom=req.nom)
        if existing_client:
            steps.append(StepResult(name="Recherche client", status="ok", detail=f"Client DMS: {existing_client['prenom']} {existing_client['nom']} (dms_id={existing_client['dms_id']})"))
        else:
            steps.append(StepResult(name="Recherche client", status="ok", detail="Nouveau client"))

        ajax_headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": self.base_url,
            "Referer": f"{self.base_url}/agenda/planningReceptionnaire.action?jbnRedirect=true",
            "Accept": "text/html, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
        }

        # Step 1: Initialize RDV form
        _log("info", "Initialisation du formulaire RDV...", "creerRdv")
        resp = self.session.post(
            f"{self.base_url}/agenda/creerRdv.action",
            data={"date": req.date, "heure": req.heure, "receptionnaireId": req.receptionnaire_id},
            headers=ajax_headers,
        )
        if resp.status_code != 200:
            _log("error", f"Echec HTTP {resp.status_code}", "creerRdv")
            steps.append(StepResult(name="Initialisation RDV", status="error", detail=f"HTTP {resp.status_code}"))
            return CreateRdvResponse(success=False, error=f"creerRdv failed: {resp.status_code}", steps=steps)
        steps.append(StepResult(name="Initialisation RDV", status="ok"))
        _log("info", "OK", "creerRdv")

        # Step 2: Check recall campaigns
        _log("info", "Verification campagnes rappel...", "campagnes")
        ts = int(datetime.now(timezone.utc).timestamp() * 1000)
        self.session.get(
            f"{self.base_url}/agenda/recupereCampagnes.action",
            params={"vehiculeDto.vin": req.vin, "_": str(ts)},
        )
        steps.append(StepResult(name="Campagnes rappel", status="ok"))
        _log("info", "OK", "campagnes")

        # Step 3: Save RDV — send actual ordre data as-is, no silent modifications
        reception_dt = f"{req.date}{req.heure}"
        restitution_dt = f"{req.date}{req.restitution_heure}"

        _log("info", f"Sauvegarde du RDV... reception={reception_dt}, restitution={restitution_dt}, duree={total_duree}h ({len(travaux)} travaux)", "sauvegarderRdv")

        save_headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": self.base_url,
            "Referer": f"{self.base_url}/agenda/planningReceptionnaire.action?jbnRedirect=true",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
        }

        payload = self._build_rdv_payload(req, reception_dt, restitution_dt, travaux=travaux, total_duree=total_duree, client_dms_id=existing_client["dms_id"] if existing_client else "")
        response = self.session.post(
            f"{self.base_url}/agenda/sauvegarderRdv.action",
            data=payload,
            headers=save_headers,
        )

        if response.status_code != 200:
            _log("error", f"Echec HTTP {response.status_code}", "sauvegarderRdv")
            steps.append(StepResult(name="Sauvegarde RDV", status="error", detail=f"HTTP {response.status_code}"))
            return CreateRdvResponse(success=False, error=f"sauvegarderRdv failed: {response.status_code}", steps=steps)

        try:
            # ServiceBox may return JSON with invalid backslash escapes (e.g. Windows paths)
            # Fix them before parsing
            raw_text = response.text
            try:
                result = json.loads(raw_text)
            except json.JSONDecodeError:
                # Replace invalid \escapes: turn lone backslashes into double backslashes
                # but preserve valid JSON escapes like \n \t \r \\ \" \/ \uXXXX
                fixed = re.sub(r'\\(?!["\\/bfnrtu])', r'\\\\', raw_text)
                result = json.loads(fixed)
                _log("warn", "Response contained invalid JSON escapes — auto-fixed", "sauvegarderRdv")
            _log("info", f"Reponse JSON statut={result.get('statut')}", "sauvegarderRdv")

            # Check for validation errors/warnings
            if result.get("statut") == "error":
                errors = result.get("data", {})
                msgs = []
                for champ in errors.get("champs", []):
                    msgs.append(f"[{champ['nom']}]: {', '.join(champ.get('detail', []))}")
                for msg in errors.get("globales", []):
                    msgs.append(msg)
                detail = "; ".join(msgs)
                _log("warn", f"Validation warnings: {detail}", "sauvegarderRdv")

                # Check if RDV was created despite warnings (dossierId present)
                retour = errors.get("retour", {}) or result.get("data", {}).get("retour", {})
                dossier_id = retour.get("diInformations", {}).get("dossierId", "") if retour else ""
                if not dossier_id:
                    # Also check at top level of data
                    dossier_id = result.get("data", {}).get("retour", {}).get("diInformations", {}).get("dossierId", "")

                if dossier_id:
                    _log("info", f"RDV cree malgre les warnings — dossierId={dossier_id}", "sauvegarderRdv")
                    steps.append(StepResult(name="Sauvegarde RDV", status="ok", detail=f"Dossier {dossier_id} (warnings: {detail})"))
                else:
                    _log("error", f"Echec: {detail}", "sauvegarderRdv")
                    # Log raw response for debugging
                    _log("info", f"Raw response: {json.dumps(result, default=str)[:500]}", "sauvegarderRdv")
                    steps.append(StepResult(name="Sauvegarde RDV", status="error", detail=detail))
                    return CreateRdvResponse(success=False, error=detail, steps=steps)
            else:
                retour = result.get("data", {}).get("retour", {})
                dossier_id = retour.get("diInformations", {}).get("dossierId", "")
                _log("info", f"OK — dossierId={dossier_id}", "sauvegarderRdv")
                steps.append(StepResult(name="Sauvegarde RDV", status="ok", detail=f"Dossier {dossier_id}"))

            # Extract rdvId from response if available
            rdv_id = ""
            if retour:
                rdv_id = str(retour.get("rdvId", retour.get("id", "")))
            if not rdv_id:
                rdv_id = str(result.get("data", {}).get("retour", {}).get("rdvId", ""))
            if rdv_id:
                _log("info", f"rdvId={rdv_id}", "sauvegarderRdv")

            used_client_id = existing_client["dms_id"] if existing_client else ""

            if not dossier_id:
                steps.append(StepResult(name="Transfert Alpha", status="skipped", detail="Pas de dossierId"))
                return CreateRdvResponse(success=True, rdv_id=rdv_id or None, client_id=used_client_id or None, error="RDV cree mais pas de dossierId pour le transfert Alpha", steps=steps)

            # Transfer to Alpha DMS
            alpha_steps = self._transfer_to_alpha(dossier_id)
            steps.extend(alpha_steps)

            or_number = None
            last = alpha_steps[-1] if alpha_steps else None
            if last and last.status == "ok" and last.name == "Reponse DMS":
                or_number = last.detail.replace("OR n°", "").strip() if last.detail.startswith("OR") else None

            return CreateRdvResponse(success=True, or_number=or_number, dossier_id=dossier_id, rdv_id=rdv_id or None, client_id=used_client_id or None, steps=steps)

        except (ValueError, KeyError) as e:
            _log("error", str(e), "sauvegarderRdv")
            steps.append(StepResult(name="Sauvegarde RDV", status="error", detail=str(e)))
            return CreateRdvResponse(success=False, error=f"Erreur parsing reponse: {e}", steps=steps)

    def _transfer_to_alpha(self, dossier_id: str) -> list[StepResult]:
        """Set basket + dmsPutDossier + RelaisServlet + dmsResponse. Returns step results."""
        steps: list[StepResult] = []

        # Step 4: Set basket
        _log("info", f"Panier -> dossier {dossier_id}...", "panier")
        set_url = f"{self.base_url}/panier/panierSetCurrent.do?id={dossier_id}"
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": f"{self.base_url}/agenda/?tabControlID=&jbnContext=true",
        }
        resp = self.session.get(set_url, headers=headers)
        if resp.status_code != 200:
            _log("error", f"Echec HTTP {resp.status_code}", "panier")
            steps.append(StepResult(name="Panier", status="error", detail=f"HTTP {resp.status_code}"))
            return steps
        steps.append(StepResult(name="Panier", status="ok"))
        _log("info", "OK", "panier")

        # Step 5: Prepare transfer
        _log("info", "Preparation du transfert...", "transfert")
        prepare_url = f"{self.base_url}/panier/panierTransfertPrepare.do?info="
        resp = self.session.get(prepare_url)
        if resp.status_code != 200:
            _log("error", f"Echec HTTP {resp.status_code}", "transfert")
            steps.append(StepResult(name="Preparation transfert", status="error", detail=f"HTTP {resp.status_code}"))
            return steps
        steps.append(StepResult(name="Preparation transfert", status="ok"))
        _log("info", "OK", "transfert")

        # Step 6: dmsPutDossier
        _log("info", "Envoi dmsPutDossier...", "dmsPut")
        payload = _parse_form_inputs(resp.text, form_id="dmsPutDossier")
        if not payload:
            _log("error", "Formulaire dmsPutDossier introuvable", "dmsPut")
            steps.append(StepResult(name="DMS Put Dossier", status="error", detail="Formulaire introuvable dans la reponse"))
            return steps
        payload = [("ajax", "true")] + [(k, v) for k, v in payload if k != "ajax"]

        post_headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": self.base_url,
            "Referer": prepare_url,
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
        }
        resp = self.session.post(
            f"{self.base_url}/panier/dmsPutDossier.do",
            data=payload,
            headers=post_headers,
        )
        if resp.status_code != 200:
            _log("error", f"Echec HTTP {resp.status_code}", "dmsPut")
            steps.append(StepResult(name="DMS Put Dossier", status="error", detail=f"HTTP {resp.status_code}"))
            return steps
        steps.append(StepResult(name="DMS Put Dossier", status="ok"))
        _log("info", "OK", "dmsPut")

        dmsput_html = resp.text

        # Step 7: RelaisServlet
        _log("info", "Envoi RelaisServlet (Alpha DMS)...", "relais")
        form_fields = _parse_form_inputs(dmsput_html, form_id="request") or _parse_form_inputs(dmsput_html, form_name="request")
        action_url = _parse_form_action(dmsput_html, form_name="request") or _parse_form_action(dmsput_html, form_id="request")
        if not action_url or not form_fields:
            _log("error", "Formulaire RelaisServlet introuvable", "relais")
            steps.append(StepResult(name="Relais Alpha", status="error", detail="Formulaire request introuvable"))
            return steps

        relais_headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": self.base_url,
            "Accept": "*/*",
        }
        resp = self.session.post(action_url, data=form_fields, headers=relais_headers, verify=False)
        if resp.status_code != 200:
            _log("error", f"Echec HTTP {resp.status_code}", "relais")
            steps.append(StepResult(name="Relais Alpha", status="error", detail=f"HTTP {resp.status_code}"))
            return steps
        steps.append(StepResult(name="Relais Alpha", status="ok"))
        _log("info", "OK", "relais")

        relais_html = resp.text

        # Step 8: dmsResponse
        _log("info", "Lecture reponse DMS...", "dmsResponse")
        response_fields = _parse_form_inputs(relais_html, form_name="response") or _parse_form_inputs(relais_html, form_id="response")
        if not response_fields:
            _log("error", "Formulaire response introuvable dans la reponse RelaisServlet", "dmsResponse")
            steps.append(StepResult(name="Reponse DMS", status="error", detail="Formulaire response introuvable"))
            return steps

        fields_dict = dict(response_fields)
        xml_value = fields_dict.get("xml", "")
        page_value = fields_dict.get("page", "13")

        dms_payload = [
            ("ajax", "true"),
            ("origine", "undefined"),
            ("current", "undefined"),
            ("xml", xml_value),
            ("type", page_value),
            ("typeInterrogation", "undefined"),
            ("typeRecherche", ""),
            ("_", ""),
        ]

        dms_headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": self.base_url,
            "Referer": prepare_url,
            "Accept": "text/javascript, text/html, application/xml, text/xml, */*",
            "X-Prototype-Version": "1.6.0.1",
            "X-Requested-With": "XMLHttpRequest",
        }

        from urllib.parse import urlencode
        resp = self.session.post(
            f"{self.base_url}/panier/dmsResponse.do",
            data=urlencode(dms_payload).encode("utf-8"),
            headers=dms_headers,
        )
        if resp.status_code != 200:
            _log("error", f"Echec HTTP {resp.status_code}", "dmsResponse")
            steps.append(StepResult(name="Reponse DMS", status="error", detail=f"HTTP {resp.status_code}"))
            return steps

        or_match = re.search(r"L'OR\s*n.\s*(\d+)", resp.text)
        if or_match:
            or_num = or_match.group(1)
            _log("info", f"OR n°{or_num}", "dmsResponse")
            steps.append(StepResult(name="Reponse DMS", status="ok", detail=f"OR n°{or_num}"))
        else:
            _log("warn", "Pas de numero OR dans la reponse", "dmsResponse")
            steps.append(StepResult(name="Reponse DMS", status="ok", detail="Pas de numero OR"))

        return steps

    def fetch_estimation(self, dossier_id: str) -> "FetchEstimationResponse":
        """Navigate to basket then fetch the estimation HTML."""
        _log("info", f"Debut fetch_estimation — dossier_id={dossier_id}, session user={self.user}", "fetchEstimation")

        # Step 1: Set basket to the given dossier
        _log("info", f"Panier -> dossier {dossier_id}...", "fetchEstimation")
        set_url = f"{self.base_url}/panier/panierSetCurrent.do?id={dossier_id}"
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": f"{self.base_url}/agenda/?tabControlID=&jbnContext=true",
        }
        try:
            resp = self.session.get(set_url, headers=headers)
        except Exception as e:
            _log("error", f"Exception panierSetCurrent: {e}", "fetchEstimation")
            return FetchEstimationResponse(success=False, error=f"panierSetCurrent exception: {e}")

        _log("info", f"panierSetCurrent status={resp.status_code}, url_finale={resp.url}, taille={len(resp.text)}", "fetchEstimation")
        if resp.status_code != 200:
            _log("error", f"Echec panierSetCurrent HTTP {resp.status_code} — body (500 premiers chars): {resp.text[:500]}", "fetchEstimation")
            return FetchEstimationResponse(success=False, error=f"panierSetCurrent HTTP {resp.status_code}")

        # Check if we got redirected to login (session expired)
        if "/login" in str(resp.url).lower() or "authentification" in resp.text[:1000].lower():
            _log("error", "Session expiree — redirection vers login detectee", "fetchEstimation")
            return FetchEstimationResponse(success=False, error="Session ServiceBox expiree. Reessayez (la session sera recree).")

        _log("info", "panierSetCurrent OK", "fetchEstimation")

        # Step 2: Fetch estimation HTML
        _log("info", "Requete printEstim.do...", "fetchEstimation")
        estim_url = f"{self.base_url}/panier/printEstim.do"
        estim_headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": self.base_url,
            "Referer": set_url,
            "Accept": "text/html, */*; q=0.01",
        }
        estim_body = "ldtIdSelected=0&typePrint=HTML&afficherRefPr=true&idAfficherRefPr=true"
        try:
            resp = self.session.post(estim_url, data=estim_body, headers=estim_headers)
        except Exception as e:
            _log("error", f"Exception printEstim: {e}", "fetchEstimation")
            return FetchEstimationResponse(success=False, error=f"printEstim exception: {e}")

        _log("info", f"printEstim status={resp.status_code}, taille={len(resp.text)}", "fetchEstimation")
        if resp.status_code != 200:
            _log("error", f"Echec printEstim HTTP {resp.status_code} — body (500 premiers chars): {resp.text[:500]}", "fetchEstimation")
            return FetchEstimationResponse(success=False, error=f"printEstim HTTP {resp.status_code}")

        # Validate we got actual estimation HTML (not an error page)
        html = resp.text
        if "Estimation" not in html and "printEstim" not in html and "DESIGNATION" not in html.upper():
            _log("warn", f"Le HTML retourne ne ressemble pas a une estimation — premiers 500 chars: {html[:500]}", "fetchEstimation")

        _log("info", f"printEstim OK — {len(html)} bytes, contient 'Estimation': {'Estimation' in html}, contient 'Sous Total': {'Sous Total' in html}", "fetchEstimation")

        return FetchEstimationResponse(success=True, html=html)

    def _build_rdv_payload(self, req: CreateRdvRequest, reception_dt: str, restitution_dt: str, travaux: list[dict] = None, total_duree: str = "0.50", client_dms_id: str = "") -> list:
        if not travaux:
            travaux = [{"nom": req.travail_nom or "Intervention", "duree": req.travail_duree}]
            total_duree = req.travail_duree
        payload = [
            ("isAppValidatedAfterDOL", ""),
            ("roleRestituteEnable", "true"),
            ("rdvDto.id", ""),
            ("rdvDto.dossierId", ""),
            ("rdv-internet", ""),
            ("rdvDto.callCenter", ""),
            ("rdvDto.rdvOlbRvi", ""),
            ("rdvDto.numORDossier", ""),
            ("rdvDto.dossierDeleted", ""),
            ("rdvDto.customerFirst", ""),
            ("rdvDto.pdvEqualId", ""),
            ("rdvDto.dhEtatRdv", ""),
            ("rdvDto.dateModifEtatRdv", ""),
            ("rdvDto.rdvInternet", ""),
            ("rdvDto.reductionInternet", ""),
            ("rdvMultiple", "false"),
            ("rdvDto.parentId", ""),
            ("isSmsBox", "true"),
            ("isSubmit", ""),
            ("isCancel", ""),
            ("rdvDto.f2mRental", ""),
            ("utilisateur.pdv.id", req.pdv_id),
            ("rdvDto.rdvOVMarque", ""),
            ("rdvDto.workLoaddistribution", ""),
            ("RdvInfosDto.isCustomerFirst", ""),
            ("rdvDto.renter", ""),
            ("rdvDto.rentername", ""),
            ("rdvDto.lead", ""),
            ("rdvDto.urgency", ""),
            ("rdvDto.codeSiteGeo", ""),
            ("rdvDto.leadMrq", ""),
            ("utilisateur.pdv.langueId", "fr_FR"),
            ("utilisateur.vauxhall", "false"),
            ("isVINDiff", "false"),
            ("utilisateur.pdv.codePays", req.code_pays),
            ("rdvDto.rdvMarque", req.marque),
            ("sms_Temp1", ""), ("sms_Temp2", ""), ("sms_Temp3", ""), ("sms_Temp4", ""), ("sms_Temp5", ""),
            # Client
            ("clientDto.id", ""),
            ("clientDto.civiliteId", req.civilite_id),
            ("clientDto.nom", req.nom),
            ("clientDto.prenom", req.prenom),
            ("clientDto.codePays", req.code_pays),
            ("clientDto.telPreference", "2"),
            ("__checkbox_clientDto.acceptationSms", "true"),
            ("clientDto.email", req.email),
            ("clientDto.lang", "fr_FR"),
            # Vehicle
            ("vehiculeDto.id", ""),
            ("vehiculeDto.marqueLibelle", req.marque_libelle),
            ("vehiculeDto.ldpLibelle", req.ldp_libelle),
            ("vehiculeDto.immatriculation", req.immatriculation),
            ("vehiculeDto.vin", req.vin),
            ("vehiculeDto.kilometrage", req.kilometrage),
            ("vehiculeDto.mec_day", req.mec_day),
            ("vehiculeDto.mec_month", req.mec_month),
            ("vehiculeDto.mec_year", req.mec_year),
            ("clientDto.idDms", client_dms_id),
            ("clientDto.telFixe", ""),
            ("clientDto.telMobile", req.tel_mobile),
            ("clientDto.telTravail", ""),
            ("clientDto.adresseRue", ""),
            ("clientDto.telMobileInternational", ""),
            ("clientDto.identifiantIndicatifTelephonique", req.indicatif_tel),
            ("adresseFormat", "123"),
            ("clientDto.numRue", ""), ("clientDto.typeRue", ""), ("clientDto.nomRue", ""),
            ("clientDto.solde", ""),
            ("clientDto.adresseCodePostal", ""), ("clientDto.adresseVille", ""),
            ("clientDto.fax", ""), ("clientDto.adresseRueComplementaire", ""),
            ("clientDto.compteDms", ""), ("clientDto.adresseComplementaire", ""),
            ("clientDto.observationDms", ""), ("clientDto.libellePays", ""),
            ("clientDto.textLibre", ""), ("clientDto.carteDeReperage", ""),
            ("vehiculeDto.idDms", ""), ("vehiculeDto.ldpCode", ""),
            ("vehiculeDto.dateMiseCirculation", "00"),
            ("isDistributedAgain", "true"), ("isDistributedAgain2", "true"),
            # Work — dynamically generated below
        ]
        # Generate one rdvTravailDtoList entry per intervention
        for i, t in enumerate(travaux):
            prefix = f"rdvTravailDtoList[{i}]"
            payload.extend([
                (f"{prefix}.id", ""),
                (f"{prefix}.travailId", ""),
                (f"{prefix}.numLdt", f"_{i}"),
                (f"{prefix}.statut", ""),
                (f"{prefix}.idExterne", ""),
                (f"{prefix}.type", ""),
                (f"{prefix}.codeSagai", ""),
                (f"{prefix}.categorie", ""),
                (f"{prefix}.dureeReference", ""),
                (f"{prefix}.ldtDateModificationTimestamp", ""),
                (f"{prefix}.initial", "true"),
                (f"{prefix}.nom", t.get("nom", "Intervention")),
                (f"{prefix}.duree", t.get("duree", "0.50")),
                (f"{prefix}.equipeId", req.equipe_id),
            ])
        # Sentinel entry after last work item
        payload.append((f"rdvTravailDtoList[{len(travaux)}].type", "mecanique"))
        # Intervention summary
        payload.extend([
            ("rdvInterventionDtoList[0].equipeId", req.equipe_id),
            ("rdvInterventionDtoList[0].statut", ""),
            ("rdvInterventionDtoList[0].dureeEstimee", total_duree),
        ])
        payload.extend([
            # RDV flags
            ("rdvDto.entretienRapide", ""), ("rdvDto.mecaniqueLourde", ""),
            ("rdvDto.diagnostic", ""), ("rdvDto.carrosserie", ""),
            ("rdvDto.imprevu", ""), ("rdvDto.pretVehicule", ""),
            ("rdvDto.attenteSurSite", ""), ("rdvDto.towedVehicle", ""),
            ("rdvDto.sensibilite", ""), ("rdvDto.retourAtelier", ""),
            ("rdvDto.selfRestitution", ""),
            ("rdvDto.typePerso1", ""), ("rdvDto.typePerso2", ""),
            ("rdvDto.typePerso3", ""), ("rdvDto.typePerso4", ""), ("rdvDto.typePerso5", ""),
            ("rdvDto.etatAvancementDto.id", "1"),
            ("rdvDto.etatAvancementDto.parent", ""),
            ("rdvDto.raisonArret", ""),
            ("rdvDto.localisationSite", "-1"),
            ("rdvDto.commentaire", ""),
            ("rdvDto.archiver", ""),
            # Planning
            ("plaDtoList[0].id", ""),
            ("plaDtoList[0].dateHeureReception", reception_dt),
            ("plaDtoList[0].typeRdvCategorie", "1"),
            ("plaDtoList[0].typeRdvId", "1"),
            ("plaDtoList[0].personnelId", req.receptionnaire_id),
            ("plaDtoList[1].id", ""),
            ("plaDtoList[1].dateHeureRestitution", restitution_dt),
            ("plaDtoList[1].typeRdvCategorie", "2"),
            ("plaDtoList[1].typeRdvId", "2"),
            ("plaDtoList[1].personnelId", req.receptionnaire_id),
            ("rdvDto.depasserCapaciteAtelier", ""),
            ("workloadoverrun_oldValName", ""),
            ("rdvMagasinDto.commentaire", ""),
            ("vehiculeBrand-[XXXX]", ""),
            ("filtreRegNumber", ""),
            # Mobility
            ("mobiliteDtoList[0].typeVehiculeAlternatifId", ""),
            ("mobiliteDtoList[0].isContratEdit", ""),
            ("mobiliteDtoList[0].isCond1Change", ""),
            ("mobiliteDtoList[0].nomConducteur1", req.nom),
            ("mobiliteDtoList[0].prenomConducteur1", req.prenom),
            ("mobiliteDtoList[0].license", ""),
            ("mobiliteDtoList[0].nomConducteur2", ""),
            ("mobiliteDtoList[0].prenomConducteur2", ""),
            ("mobiliteDtoList[0].idPret", ""),
            ("mobiliteDtoList[0].statut", ""),
            ("mobiliteDtoList[0].pretDebut.id", ""),
            ("mobiliteDtoList[0].pretDebut.typeRdvCategorie", "1"),
            ("mobiliteDtoList[0].pretDebut.typeRdvId", "1"),
            ("mobiliteDtoList[0].pretDebut.dateHeureReception", reception_dt),
            ("mobiliteDtoList[0].pretFin.id", ""),
            ("mobiliteDtoList[0].pretFin.typeRdvCategorie", "2"),
            ("mobiliteDtoList[0].pretFin.typeRdvId", "2"),
            ("mobiliteDtoList[0].pretFin.dateHeureRestitution", restitution_dt),
            ("mobiliteDtoList[0].pretDebut.personnelId", req.receptionnaire_id),
            ("mobiliteDtoList[0].kmDepartStringValue", ""),
            ("mobiliteDtoList[0].bookingId", ""), ("mobiliteDtoList[0].bookingKey", ""),
            ("mobiliteDtoList[0].bookingVehSource", ""), ("mobiliteDtoList[0].bookingName", ""),
            ("mobiliteDtoList[0].bookingReferance", ""),
            ("mobiliteDtoList[0].pretFin.personnelId", req.receptionnaire_id),
            ("mobiliteDtoList[0].kmRetourStringValue", ""),
            ("vehiculeBrand-[0]", ""),
            ("mobiliteDtoList[0].etatVehiculeCommentaire", ""),
            ("mobiliteDtoList[0].commentaire", ""),
            # SMS
            ("saisieSmsDto.messageTexte", ""),
            ("compteur", "0"),
            ("maxLengthSMS", "160"),
            ("saisieSmsDto.messageDateProgramme", ""),
        ])
        return payload


# ─── HTML Parsing helpers ────────────────────────────────────────────────

def _parse_form_inputs(html: str, form_id: str = None, form_name: str = None) -> list:
    target = form_id or form_name

    class FormParser(HTMLParser):
        def __init__(self, target):
            super().__init__()
            self.target = target
            self.in_form = False
            self.fields = []
            self._current_select = ""

        def handle_starttag(self, tag, attrs):
            a = dict(attrs)
            if tag == "form":
                if a.get("id") == self.target or a.get("name") == self.target:
                    self.in_form = True
            if self.in_form and tag == "input":
                name = a.get("name", "")
                value = a.get("value", "")
                if name:
                    self.fields.append((name, value))
            if self.in_form and tag == "select":
                self._current_select = a.get("name", "")
            if self.in_form and tag == "option":
                if "selected" in a and self._current_select:
                    self.fields.append((self._current_select, a.get("value", "")))

        def handle_endtag(self, tag):
            if tag == "form" and self.in_form:
                self.in_form = False

    parser = FormParser(target)
    parser.feed(html)
    return parser.fields


def _parse_form_action(html: str, form_name: str = None, form_id: str = None) -> Optional[str]:
    target = form_name or form_id

    class ActionParser(HTMLParser):
        def __init__(self, target):
            super().__init__()
            self.target = target
            self.action = None

        def handle_starttag(self, tag, attrs):
            if tag == "form":
                a = dict(attrs)
                if a.get("name") == self.target or a.get("id") == self.target:
                    self.action = a.get("action", "")

    parser = ActionParser(target)
    parser.feed(html)
    return parser.action


# ─── Credential persistence (auto-bootstrap on restart) ──────────────────

_CREDS_FILE = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0] if not getattr(sys, "frozen", False) else sys.executable)), "_saved_creds.json")


def _save_creds(username: str, password: str):
    """Persist credentials so the proxy can auto-bootstrap after restart."""
    try:
        with open(_CREDS_FILE, "w") as f:
            json.dump({"username": username, "password": password}, f)
    except Exception as e:
        _log("warn", f"Impossible de sauvegarder les credentials: {e}", "creds")


def _load_creds() -> dict | None:
    """Load saved credentials, or None."""
    try:
        with open(_CREDS_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return None


# ─── Helper to get or create session ─────────────────────────────────────

def _get_session(creds: Credentials) -> ServiceBoxSession:
    key = creds.username
    if key not in _sessions:
        s = ServiceBoxSession(creds.username, creds.password)
        s.bootstrap()
        _sessions[key] = s
        _save_creds(creds.username, creds.password)
    return _sessions[key]


# ─── Session keep-alive (prevent Windows/RDP inactivity disconnect) ──────

# Simulate a harmless key press (F15 — exists in the HID spec but no
# physical key on any keyboard, so it won't interfere with anything).
# This resets the Windows idle timer, preventing:
#   - Screen lock
#   - RDP/VPN session disconnect due to inactivity

ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001
ES_DISPLAY_REQUIRED = 0x00000002

SESSION_KEEPALIVE_INTERVAL = 60  # seconds


def _session_keepalive_loop():
    """Prevent the Windows session from going idle by poking the OS every minute."""
    while True:
        time.sleep(SESSION_KEEPALIVE_INTERVAL)
        try:
            # Tell Windows the system is not idle (prevents sleep + screen lock)
            ctypes.windll.kernel32.SetThreadExecutionState(
                ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_DISPLAY_REQUIRED
            )
            _log("debug", "Session keep-alive: execution state reset", "keepalive")
        except Exception as e:
            _log("error", f"Session keep-alive failed: {e}", "keepalive")


_session_keepalive_thread = threading.Thread(target=_session_keepalive_loop, daemon=True)
_session_keepalive_thread.start()
_log("info", f"Session keep-alive started (every {SESSION_KEEPALIVE_INTERVAL}s)", "keepalive")


# ─── Periodic SSO token sync ────────────────────────────────────────────

SSO_SYNC_INTERVAL = 20 * 60  # 20 minutes


def _ensure_session_exists():
    """If no sessions exist, try to bootstrap from saved credentials."""
    if _sessions:
        return
    saved = _load_creds()
    if not saved:
        return
    username, password = saved["username"], saved["password"]
    _log("info", f"Auto-bootstrap avec credentials sauvegardees ({username})...", "ssoSync")
    try:
        s = ServiceBoxSession(username, password)
        s.bootstrap()
        _sessions[username] = s
    except Exception as e:
        _log("warn", f"Auto-bootstrap echoue: {e}", "ssoSync")


def _sso_sync_loop():
    """Re-sync SSO tokens every 20 minutes. Auto-bootstraps if needed."""
    # Initial sync on startup (after short delay for proxy to be ready)
    time.sleep(10)
    _ensure_session_exists()

    while True:
        time.sleep(SSO_SYNC_INTERVAL)
        _ensure_session_exists()
        for username, session in list(_sessions.items()):
            try:
                _log("info", f"Re-sync SSO token pour {username}...", "ssoSync")
                session._sync_sso_token()
            except Exception as e:
                _log("warn", f"SSO re-sync echoue pour {username}: {e}", "ssoSync")


_sso_sync_thread = threading.Thread(target=_sso_sync_loop, daemon=True)
_sso_sync_thread.start()
_log("info", f"SSO sync started (every {SSO_SYNC_INTERVAL // 60}min)", "ssoSync")


# ─── Routes ──────────────────────────────────────────────────────────────

@app.get("/help")
def help_endpoint():
    """List all available endpoints."""
    return {"version": VERSION, "endpoints": [
        {"method": "GET",  "path": "/health",            "description": "Statut du proxy (version, nb sessions)"},
        {"method": "GET",  "path": "/help",              "description": "Liste des endpoints disponibles"},
        {"method": "GET",  "path": "/logs",              "description": "Derniers logs en memoire (?limit=200)"},
        {"method": "GET",  "path": "/docs",              "description": "Swagger UI (ouvrir dans un navigateur)"},
        {"method": "GET",  "path": "/verify-browser",    "description": "Page HTML pour tester ServiceBox dans le navigateur"},
        {"method": "POST", "path": "/test-connection",   "description": "Teste la connexion et l'authentification ServiceBox"},
        {"method": "POST", "path": "/options",           "description": "Recupere receptionnaires + equipes pour creation RDV"},
        {"method": "POST", "path": "/create-rdv",        "description": "Cree un RDV complet (client + vehicule + transfert Alpha)"},
        {"method": "POST", "path": "/search-client",     "description": "Recherche un client existant dans Alpha DMS"},
        {"method": "POST", "path": "/delete-rdv",        "description": "Supprime un RDV par ID"},
        {"method": "POST", "path": "/reset-session",     "description": "Force le re-bootstrap de la session"},
        {"method": "POST", "path": "/fetch-estimation",  "description": "Recupere le HTML d'estimation pour un dossier"},
        {"method": "POST", "path": "/debug-auth",        "description": "Teste toutes les methodes d'auth (diagnostic)"},
        {"method": "POST", "path": "/force-update",      "description": "Declenche une mise a jour immediate"},
        {"method": "GET",  "path": "/operations",        "description": "Historique des operations (?operation=&username=&success=&date_from=&date_to=&limit=&offset=)"},
        {"method": "GET",  "path": "/operations/export", "description": "Export JSON ou CSV (?format=csv&date_from=&date_to=)"},
        {"method": "GET",  "path": "/operations/stats",  "description": "Statistiques par type d'operation (?date_from=&date_to=)"},
    ]}


@app.get("/health")
def health():
    return {"status": "ok", "version": VERSION, "sessions": len(_sessions)}


@app.post("/force-update")
def force_update():
    """Trigger an immediate update check. If a newer version exists, download and restart."""
    from updater import check_for_update, apply_update
    release = check_for_update()
    if not release:
        return {"updated": False, "message": f"Deja a jour (v{VERSION})"}
    result = apply_update(release)
    if result == "service":
        return {"updated": True, "message": f"Mise a jour vers {release['tag_name']} lancee (redemarrage service)"}
    elif result == "standalone":
        import os
        threading.Thread(target=lambda: (time.sleep(1), os._exit(0)), daemon=True).start()
        return {"updated": True, "message": f"Mise a jour vers {release['tag_name']} lancee (redemarrage)"}
    return {"updated": False, "message": f"Version {release['tag_name']} disponible mais non applicable (mode dev)"}


@app.get("/verify-browser")
def verify_browser():
    """HTML page to verify ServiceBox credentials in the browser on this machine."""
    from fastapi.responses import HTMLResponse
    html = """<!DOCTYPE html>
<html><head><title>ServiceBox Proxy - Browser Verification</title>
<style>body{font-family:system-ui;max-width:600px;margin:40px auto;padding:20px}
button{padding:10px 20px;font-size:16px;cursor:pointer;margin:5px}
.result{margin-top:20px;padding:15px;border-radius:8px;font-family:monospace;white-space:pre-wrap}
.ok{background:#F0FDF4;border:1px solid #BBF7D0;color:#166534}
.err{background:#FEF2F2;border:1px solid #FECACA;color:#991B1B}</style></head>
<body>
<h2>ServiceBox Proxy v""" + VERSION + """ - Verification navigateur</h2>
<p>Ce test verifie si votre navigateur peut se connecter a ServiceBox depuis cette machine.</p>
<p><strong>Cliquez le bouton ci-dessous.</strong> Si une popup d'authentification apparait,
entrez vos identifiants ServiceBox (DF09057 / Jeje2523).</p>
<button onclick="testBrowser()">Tester ServiceBox dans le navigateur</button>
<button onclick="testFetch()">Tester avec fetch() (comme le proxy)</button>
<div id="result"></div>
<script>
function testBrowser() {
  document.getElementById('result').innerHTML = '<div class="result">Ouverture de ServiceBox...</div>';
  window.open('https://servicebox.mpsa.com/agenda/planningReceptionnaire.action', '_blank');
}
async function testFetch() {
  const el = document.getElementById('result');
  el.innerHTML = '<div class="result">Test en cours...</div>';
  const user = 'DF09057', pass = 'Jeje2523';
  const b64 = btoa(user + ':' + pass);
  try {
    const r = await fetch('https://servicebox.mpsa.com/agenda/planningReceptionnaire.action', {
      headers: { 'Authorization': 'Basic ' + b64 },
    });
    el.innerHTML = '<div class="result ' + (r.ok ? 'ok' : 'err') + '">fetch(): HTTP ' + r.status + ' (' + r.statusText + ')\\n' +
      'Content-Length: ' + (r.headers.get('content-length') || '?') + '</div>';
  } catch(e) {
    el.innerHTML = '<div class="result err">fetch() error: ' + e.message + '\\n(CORS bloque probablement)</div>';
  }
}
</script>
</body></html>"""
    return HTMLResponse(content=html)


@app.get("/logs")
def get_logs(limit: int = 200):
    """Return the last N log entries (newest first)."""
    entries = list(_log_entries)[-limit:]
    entries.reverse()
    return {"logs": entries}


@app.post("/test-connection", response_model=TestConnectionResponse)
def test_connection(creds: Credentials):
    """Test that we can reach ServiceBox and authenticate."""
    t0 = time.time()
    result = TestConnectionResponse(connected=False, session_ok=False, servicebox_reachable=False)

    # Test 1: Can we reach servicebox.mpsa.com at all?
    _log("info", "Test de connexion a ServiceBox...", "test")
    try:
        r = requests.get("https://servicebox.mpsa.com", timeout=10, allow_redirects=False, verify=False)
        # Any HTTP response means the server is reachable (even 401/403)
        result.servicebox_reachable = True
        _log("info", f"Accessible (HTTP {r.status_code})", "test")
    except Exception as e:
        _log("error", f"Injoignable: {e}", "test")
        result.detail = f"ServiceBox injoignable: {e}"
        _ops_record("test_connection", creds.username, success=False, error=result.detail, duration_ms=int((time.time() - t0) * 1000))
        return result

    # Test 2: Can we authenticate and get a session?
    _log("info", "Test d'authentification...", "test")
    try:
        session = ServiceBoxSession(creds.username, creds.password)
        session.bootstrap()

        # Try to load the agenda page — if auth works, we get HTML with our user info
        test_resp = session.session.get(
            f"{session.base_url}/agenda/planningReceptionnaire.action",
            timeout=15,
        )
        _log("info", f"Reponse auth: HTTP {test_resp.status_code} ({len(test_resp.text)} bytes)", "test")
        if test_resp.status_code == 200 and len(test_resp.text) > 500:
            result.session_ok = True
            result.connected = True
            result.detail = "Connexion et authentification OK"
            _log("info", "Authentification OK", "test")
            # Cache the session
            _sessions[creds.username] = session
        elif test_resp.status_code == 401:
            result.detail = "Identifiants ServiceBox incorrects (HTTP 401). Verifiez username/password dans les parametres."
            _log("error", result.detail, "test")
        else:
            result.detail = f"Authentification echouee (HTTP {test_resp.status_code}, {len(test_resp.text)} bytes)"
            _log("error", result.detail, "test")
    except Exception as e:
        result.detail = f"Erreur d'authentification: {e}"
        _log("error", result.detail, "test")

    _ops_record(
        "test_connection", creds.username,
        response_data={"connected": result.connected, "session_ok": result.session_ok, "detail": result.detail},
        success=result.connected, error="" if result.connected else result.detail,
        duration_ms=int((time.time() - t0) * 1000),
    )
    return result


@app.post("/options", response_model=AgendaOptionsResponse)
def get_options(req: AgendaOptionsRequest):
    t0 = time.time()
    try:
        session = _get_session(req)
        result = session.get_agenda_options()
        # If both lists are empty, the session is likely stale — re-bootstrap once
        if not result["receptionnaires"] and not result["equipes"]:
            _log("warn", "Options vides, re-bootstrap de la session...", "options")
            _sessions.pop(req.username, None)
            session = _get_session(req)
            result = session.get_agenda_options()
        _ops_record(
            "options", req.username,
            response_data={"receptionnaires": len(result["receptionnaires"]), "equipes": len(result["equipes"])},
            duration_ms=int((time.time() - t0) * 1000),
        )
        return result
    except HTTPException:
        _ops_record("options", req.username, success=False, error="HTTPException", duration_ms=int((time.time() - t0) * 1000))
        raise
    except Exception as e:
        traceback.print_exc()
        _ops_record("options", req.username, success=False, error=str(e), duration_ms=int((time.time() - t0) * 1000))
        raise HTTPException(500, str(e))


@app.post("/create-rdv", response_model=CreateRdvResponse)
def create_rdv(req: CreateRdvRequest):
    t0 = time.time()
    try:
        session = _get_session(req)
        result = session.create_rdv(req)
        _ops_record(
            "create_rdv", req.username,
            request_data=_ops_safe_request(req),
            response_data={
                "success": result.success,
                "or_number": result.or_number,
                "dossier_id": result.dossier_id,
                "rdv_id": result.rdv_id,
                "client_id": result.client_id,
                "error": result.error,
                "steps": [s.dict() for s in result.steps],
            },
            success=result.success,
            error=result.error or "",
            duration_ms=int((time.time() - t0) * 1000),
        )
        return result
    except Exception as e:
        traceback.print_exc()
        _ops_record("create_rdv", req.username, request_data=_ops_safe_request(req), success=False, error=str(e), duration_ms=int((time.time() - t0) * 1000))
        return CreateRdvResponse(success=False, error=str(e))


class SearchClientRequest(Credentials):
    phone: str = ""
    nom: str = ""


class SearchClientResponse(BaseModel):
    found: bool
    dms_id: Optional[str] = None
    nom: Optional[str] = None
    prenom: Optional[str] = None
    detail: str = ""


@app.post("/search-client", response_model=SearchClientResponse)
def search_client(req: SearchClientRequest):
    t0 = time.time()
    try:
        session = _get_session(req)
        result = session._search_client_dms(req.phone, nom=req.nom)
        if result:
            resp = SearchClientResponse(
                found=True,
                dms_id=result["dms_id"],
                nom=result["nom"],
                prenom=result["prenom"],
                detail=f"Client DMS: {result['prenom']} {result['nom']} (id={result['dms_id']})",
            )
            _ops_record(
                "search_client", req.username,
                request_data={"phone": req.phone, "nom": req.nom},
                response_data={"found": True, "dms_id": result["dms_id"], "nom": result["nom"], "prenom": result["prenom"]},
                duration_ms=int((time.time() - t0) * 1000),
            )
            return resp
        _ops_record(
            "search_client", req.username,
            request_data={"phone": req.phone, "nom": req.nom},
            response_data={"found": False},
            duration_ms=int((time.time() - t0) * 1000),
        )
        return SearchClientResponse(found=False, detail="Aucun client trouve")
    except Exception as e:
        traceback.print_exc()
        _ops_record("search_client", req.username, request_data={"phone": req.phone, "nom": req.nom}, success=False, error=str(e), duration_ms=int((time.time() - t0) * 1000))
        return SearchClientResponse(found=False, detail=str(e))


class DeleteRdvRequest(Credentials):
    rdv_id: str


@app.post("/delete-rdv")
def delete_rdv(req: DeleteRdvRequest):
    t0 = time.time()
    try:
        session = _get_session(req)
        result = session.delete_rdv(req.rdv_id)
        is_ok = result.get("success", False) if isinstance(result, dict) else True
        _ops_record(
            "delete_rdv", req.username,
            request_data={"rdv_id": req.rdv_id},
            response_data=result if isinstance(result, dict) else {"result": str(result)},
            success=is_ok,
            error=result.get("error", "") if isinstance(result, dict) and not is_ok else "",
            duration_ms=int((time.time() - t0) * 1000),
        )
        return result
    except Exception as e:
        traceback.print_exc()
        _ops_record("delete_rdv", req.username, request_data={"rdv_id": req.rdv_id}, success=False, error=str(e), duration_ms=int((time.time() - t0) * 1000))
        return {"success": False, "error": str(e)}


@app.post("/reset-session")
def reset_session(creds: Credentials):
    """Force re-bootstrap (useful if session expired)."""
    t0 = time.time()
    key = creds.username
    _sessions.pop(key, None)
    session = _get_session(creds)
    _ops_record("reset_session", creds.username, duration_ms=int((time.time() - t0) * 1000))
    return {"status": "ok"}


@app.post("/debug-auth")
def debug_auth(creds: Credentials):
    """Try every possible auth method and report results for debugging."""
    import http.client
    import ssl
    import socket
    import urllib.request

    results = []
    url = "https://servicebox.mpsa.com/agenda/planningReceptionnaire.action"
    host = "servicebox.mpsa.com"
    path = "/agenda/planningReceptionnaire.action"
    auth_str = f"{creds.username}:{creds.password}"
    b64_auth = base64.b64encode(auth_str.encode()).decode()

    # 0. DNS resolution
    try:
        ip = socket.gethostbyname(host)
        results.append({"test": "DNS", "result": f"ok", "detail": f"{host} -> {ip}"})
        _log("info", f"DNS: {host} -> {ip}", "debug")
    except Exception as e:
        results.append({"test": "DNS", "result": "error", "detail": str(e)})

    # 1. System proxy settings
    try:
        proxies = urllib.request.getproxies()
        results.append({"test": "System proxies", "result": "info", "detail": json.dumps(proxies)})
        _log("info", f"System proxies: {proxies}", "debug")
    except Exception as e:
        results.append({"test": "System proxies", "result": "error", "detail": str(e)})

    # 1b. Show exact auth header for manual verification
    results.append({
        "test": "Auth header sent",
        "result": "info",
        "detail": f"Authorization: Basic {b64_auth} (decoded: {creds.username}:{creds.password})",
    })
    _log("info", f"Auth header: Basic {b64_auth}", "debug")

    # 1c. WinHTTP proxy settings (separate from WinINET used by browsers)
    try:
        winhttp = subprocess.run(
            ["netsh", "winhttp", "show", "proxy"],
            capture_output=True, text=True, timeout=5,
        )
        results.append({
            "test": "WinHTTP proxy",
            "result": "info",
            "detail": winhttp.stdout.strip(),
        })
        _log("info", f"WinHTTP: {winhttp.stdout.strip()}", "debug")
    except Exception as e:
        results.append({"test": "WinHTTP proxy", "result": "error", "detail": str(e)})

    # 1d. PowerShell Invoke-WebRequest (.NET/WinHTTP stack — different from everything else)
    _log("info", "Test avec PowerShell Invoke-WebRequest...", "debug")
    try:
        ps_script = (
            f"$pair = '{creds.username}:{creds.password}'; "
            f"$bytes = [System.Text.Encoding]::ASCII.GetBytes($pair); "
            f"$b64 = [System.Convert]::ToBase64String($bytes); "
            f"$headers = @{{ Authorization = \"Basic $b64\"; 'User-Agent' = 'Mozilla/5.0' }}; "
            f"try {{ "
            f"  $r = Invoke-WebRequest -Uri '{url}' -Headers $headers -UseBasicParsing -TimeoutSec 15; "
            f"  Write-Output \"HTTP $($r.StatusCode) $($r.Content.Length) bytes\" "
            f"}} catch {{ "
            f"  $e = $_.Exception; "
            f"  if ($e.Response) {{ "
            f"    $code = [int]$e.Response.StatusCode; "
            f"    Write-Output \"HTTP $code\" "
            f"  }} else {{ "
            f"    Write-Output \"ERROR: $($e.Message)\" "
            f"  }} "
            f"}}"
        )
        ps_result = subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps_script],
            capture_output=True, text=True, timeout=25,
        )
        ps_out = ps_result.stdout.strip()
        results.append({
            "test": "PowerShell (.NET)",
            "result": ps_out or "no output",
            "detail": ps_result.stderr.strip()[:300] if ps_result.stderr else "",
        })
        _log("info", f"PowerShell: {ps_out}", "debug")
    except Exception as e:
        results.append({"test": "PowerShell (.NET)", "result": "error", "detail": str(e)})

    # 1e. PowerShell with -UseDefaultCredentials (Windows Integrated Auth / Kerberos)
    _log("info", "Test PowerShell avec Windows Integrated Auth...", "debug")
    try:
        ps_script2 = (
            f"try {{ "
            f"  $r = Invoke-WebRequest -Uri '{url}' -UseDefaultCredentials -UseBasicParsing -TimeoutSec 15; "
            f"  Write-Output \"HTTP $($r.StatusCode) $($r.Content.Length) bytes\" "
            f"}} catch {{ "
            f"  $e = $_.Exception; "
            f"  if ($e.Response) {{ "
            f"    $code = [int]$e.Response.StatusCode; "
            f"    Write-Output \"HTTP $code\" "
            f"  }} else {{ "
            f"    Write-Output \"ERROR: $($e.Message)\" "
            f"  }} "
            f"}}"
        )
        ps_result2 = subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps_script2],
            capture_output=True, text=True, timeout=25,
        )
        ps_out2 = ps_result2.stdout.strip()
        results.append({
            "test": "PowerShell (Windows Auth)",
            "result": ps_out2 or "no output",
            "detail": ps_result2.stderr.strip()[:300] if ps_result2.stderr else "",
        })
        _log("info", f"PowerShell WinAuth: {ps_out2}", "debug")
    except Exception as e:
        results.append({"test": "PowerShell (Windows Auth)", "result": "error", "detail": str(e)})

    # 2. Raw http.client (bypass requests library entirely)
    _log("info", "Test avec http.client (raw)...", "debug")
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        conn = http.client.HTTPSConnection(host, timeout=15, context=ctx)
        conn.request("GET", path, headers={
            "Authorization": f"Basic {b64_auth}",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Host": host,
        })
        resp = conn.getresponse()
        body = resp.read()
        hdrs = dict(resp.getheaders())
        results.append({
            "test": "http.client direct",
            "result": "ok" if resp.status == 200 else f"HTTP {resp.status}",
            "detail": f"{len(body)} bytes, headers={json.dumps(hdrs)}",
        })
        _log("info", f"http.client: HTTP {resp.status} ({len(body)} bytes)", "debug")
        conn.close()
    except Exception as e:
        results.append({"test": "http.client direct", "result": "error", "detail": str(e)})
        _log("error", f"http.client: {e}", "debug")

    # 3. requests with NO proxy (trust_env=False)
    _log("info", "Test requests sans proxy...", "debug")
    try:
        s = requests.Session()
        s.trust_env = False
        s.verify = False
        r = s.get(url, headers={
            "Authorization": f"Basic {b64_auth}",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }, timeout=15)
        results.append({
            "test": "requests (no proxy)",
            "result": f"HTTP {r.status_code}",
            "detail": f"{len(r.text)} bytes",
        })
        _log("info", f"requests no-proxy: HTTP {r.status_code} ({len(r.text)} bytes)", "debug")
    except Exception as e:
        results.append({"test": "requests (no proxy)", "result": "error", "detail": str(e)})

    # 4. requests with explicit system proxy
    _log("info", "Test requests avec proxy systeme...", "debug")
    try:
        proxies = urllib.request.getproxies()
        s = requests.Session()
        s.verify = False
        if proxies:
            s.proxies.update(proxies)
        r = s.get(url, headers={
            "Authorization": f"Basic {b64_auth}",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }, timeout=15)
        results.append({
            "test": "requests (system proxy)",
            "result": f"HTTP {r.status_code}",
            "detail": f"{len(r.text)} bytes, proxies={json.dumps(proxies)}",
        })
        _log("info", f"requests system-proxy: HTTP {r.status_code} ({len(r.text)} bytes)", "debug")
    except Exception as e:
        results.append({"test": "requests (system proxy)", "result": "error", "detail": str(e)})

    # 5. curl subprocess — uses Windows SChannel (native TLS + cert store like browser)
    _log("info", "Test avec curl (--ssl-native)...", "debug")
    try:
        # curl.exe on Windows uses SChannel by default = same TLS as browser
        # -v for verbose TLS handshake info
        curl_result = subprocess.run(
            ["curl.exe", "-sS", "-v", "--ssl-no-revoke",
             "-w", "\n%{http_code}\n%{size_download}",
             "-u", f"{creds.username}:{creds.password}",
             "-H", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
             url],
            capture_output=True, text=True, timeout=20,
        )
        lines = curl_result.stdout.strip().split("\n")
        status = lines[-2] if len(lines) >= 2 else "?"
        size = lines[-1] if len(lines) >= 1 else "?"
        body_preview = curl_result.stdout[:300] if curl_result.stdout else ""
        results.append({
            "test": "curl (SChannel)",
            "result": f"HTTP {status}",
            "detail": f"{size} bytes, body={body_preview[:200]}",
        })
        _log("info", f"curl: HTTP {status} ({size} bytes)", "debug")
        # stderr contains verbose TLS handshake details
        if curl_result.stderr:
            stderr_text = curl_result.stderr[:1000]
            _log("info", f"curl verbose: {stderr_text}", "debug")
            results.append({"test": "curl verbose (TLS handshake)", "result": "info", "detail": stderr_text})
    except FileNotFoundError:
        results.append({"test": "curl", "result": "skip", "detail": "curl.exe not found"})
        _log("warn", "curl.exe non trouve", "debug")
    except Exception as e:
        results.append({"test": "curl", "result": "error", "detail": str(e)})
        _log("error", f"curl: {e}", "debug")

    # 6. Check for client certificates in Windows cert store
    _log("info", "Verification certificats client Windows...", "debug")
    try:
        certutil_result = subprocess.run(
            ["certutil", "-user", "-store", "My"],
            capture_output=True, text=True, timeout=10,
        )
        # Count certs
        cert_count = certutil_result.stdout.count("Serial Number:")
        cert_subjects = re.findall(r"Subject:\s*(.+)", certutil_result.stdout)
        results.append({
            "test": "Windows client certs (user store)",
            "result": f"{cert_count} certs",
            "detail": "; ".join(cert_subjects[:5]) if cert_subjects else "aucun certificat",
        })
        _log("info", f"Client certs: {cert_count} trouves", "debug")
        for subj in cert_subjects[:5]:
            _log("info", f"  Cert: {subj}", "debug")
    except Exception as e:
        results.append({"test": "Windows client certs", "result": "error", "detail": str(e)})

    # 7. Check machine cert store too
    try:
        certutil_result = subprocess.run(
            ["certutil", "-store", "My"],
            capture_output=True, text=True, timeout=10,
        )
        cert_count = certutil_result.stdout.count("Serial Number:")
        cert_subjects = re.findall(r"Subject:\s*(.+)", certutil_result.stdout)
        results.append({
            "test": "Windows client certs (machine store)",
            "result": f"{cert_count} certs",
            "detail": "; ".join(cert_subjects[:5]) if cert_subjects else "aucun certificat",
        })
        _log("info", f"Machine certs: {cert_count} trouves", "debug")
    except Exception as e:
        results.append({"test": "Windows machine certs", "result": "error", "detail": str(e)})

    # 8. Try urllib.request
    _log("info", "Test avec urllib.request...", "debug")
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        req = urllib.request.Request(url, headers={
            "Authorization": f"Basic {b64_auth}",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        })
        resp = urllib.request.urlopen(req, timeout=15, context=ctx)
        body = resp.read()
        results.append({
            "test": "urllib.request",
            "result": f"HTTP {resp.status}",
            "detail": f"{len(body)} bytes",
        })
        _log("info", f"urllib: HTTP {resp.status} ({len(body)} bytes)", "debug")
    except urllib.error.HTTPError as e:
        results.append({
            "test": "urllib.request",
            "result": f"HTTP {e.code}",
            "detail": f"headers={dict(e.headers)}",
        })
        _log("info", f"urllib: HTTP {e.code}", "debug")
    except Exception as e:
        results.append({"test": "urllib.request", "result": "error", "detail": str(e)})

    # 9. Python TLS version info
    _log("info", "Info TLS Python...", "debug")
    try:
        results.append({
            "test": "Python TLS",
            "result": "info",
            "detail": f"OpenSSL: {ssl.OPENSSL_VERSION}, default protocol: {ssl.PROTOCOL_TLS}",
        })
    except Exception as e:
        results.append({"test": "Python TLS", "result": "error", "detail": str(e)})

    return {"results": results}


@app.post("/fetch-estimation", response_model=FetchEstimationResponse)
def fetch_estimation(req: FetchEstimationRequest):
    t0 = time.time()
    try:
        session = _get_session(req)
        result = session.fetch_estimation(req.dossier_id)
        _ops_record(
            "fetch_estimation", req.username,
            request_data={"dossier_id": req.dossier_id},
            response_data={"success": result.success, "has_html": result.html is not None, "error": result.error},
            success=result.success, error=result.error or "",
            duration_ms=int((time.time() - t0) * 1000),
        )
        return result
    except Exception as e:
        _log("error", str(e), "fetchEstimation")
        _ops_record("fetch_estimation", req.username, request_data={"dossier_id": req.dossier_id}, success=False, error=str(e), duration_ms=int((time.time() - t0) * 1000))
        return FetchEstimationResponse(success=False, error=str(e))


# ─── Operations query endpoints ──────────────────────────────────────────

@app.get("/operations")
def get_operations(
    operation: Optional[str] = None,
    username: Optional[str] = None,
    success: Optional[bool] = None,
    date_from: Optional[str] = None,   # ISO date, e.g. 2026-04-01
    date_to: Optional[str] = None,     # ISO date, e.g. 2026-04-12
    limit: int = 100,
    offset: int = 0,
):
    """Query operations with optional filters. Returns newest first."""
    con = sqlite3.connect(_ops_db_path())
    con.row_factory = sqlite3.Row
    clauses, params = [], []
    if operation:
        clauses.append("operation = ?")
        params.append(operation)
    if username:
        clauses.append("username = ?")
        params.append(username)
    if success is not None:
        clauses.append("success = ?")
        params.append(1 if success else 0)
    if date_from:
        clauses.append("timestamp >= ?")
        params.append(date_from)
    if date_to:
        clauses.append("timestamp < ?")
        params.append(date_to + "T23:59:59")
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""

    # Total count
    total = con.execute(f"SELECT COUNT(*) FROM operations{where}", params).fetchone()[0]

    rows = con.execute(
        f"SELECT * FROM operations{where} ORDER BY id DESC LIMIT ? OFFSET ?",
        params + [limit, offset],
    ).fetchall()
    con.close()

    items = []
    for r in rows:
        items.append({
            "id": r["id"],
            "timestamp": r["timestamp"],
            "operation": r["operation"],
            "username": r["username"],
            "request_data": json.loads(r["request_data"]),
            "response_data": json.loads(r["response_data"]),
            "success": bool(r["success"]),
            "error": r["error"],
            "duration_ms": r["duration_ms"],
        })
    return {"total": total, "limit": limit, "offset": offset, "operations": items}


@app.get("/operations/export")
def export_operations(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    format: str = "json",  # "json" or "csv"
):
    """Export all operations for a date range as JSON array or CSV."""
    con = sqlite3.connect(_ops_db_path())
    con.row_factory = sqlite3.Row
    clauses, params = [], []
    if date_from:
        clauses.append("timestamp >= ?")
        params.append(date_from)
    if date_to:
        clauses.append("timestamp < ?")
        params.append(date_to + "T23:59:59")
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""

    rows = con.execute(f"SELECT * FROM operations{where} ORDER BY id DESC", params).fetchall()
    con.close()

    if format == "csv":
        from fastapi.responses import PlainTextResponse
        import csv
        import io
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["id", "timestamp", "operation", "username", "request_data", "response_data", "success", "error", "duration_ms"])
        for r in rows:
            writer.writerow([r["id"], r["timestamp"], r["operation"], r["username"], r["request_data"], r["response_data"], bool(r["success"]), r["error"], r["duration_ms"]])
        return PlainTextResponse(buf.getvalue(), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=operations.csv"})

    items = []
    for r in rows:
        items.append({
            "id": r["id"],
            "timestamp": r["timestamp"],
            "operation": r["operation"],
            "username": r["username"],
            "request_data": json.loads(r["request_data"]),
            "response_data": json.loads(r["response_data"]),
            "success": bool(r["success"]),
            "error": r["error"],
            "duration_ms": r["duration_ms"],
        })
    return {"operations": items}


@app.get("/operations/stats")
def operations_stats(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
):
    """Summary counts per operation type, success rate, average duration."""
    con = sqlite3.connect(_ops_db_path())
    clauses, params = [], []
    if date_from:
        clauses.append("timestamp >= ?")
        params.append(date_from)
    if date_to:
        clauses.append("timestamp < ?")
        params.append(date_to + "T23:59:59")
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""

    rows = con.execute(f"""
        SELECT operation,
               COUNT(*)           AS total,
               SUM(success)       AS successes,
               COUNT(*) - SUM(success) AS failures,
               ROUND(AVG(duration_ms)) AS avg_duration_ms
        FROM operations{where}
        GROUP BY operation
        ORDER BY total DESC
    """, params).fetchall()

    total_row = con.execute(f"""
        SELECT COUNT(*) AS total, SUM(success) AS successes,
               ROUND(AVG(duration_ms)) AS avg_duration_ms
        FROM operations{where}
    """, params).fetchone()
    con.close()

    by_operation = []
    for r in rows:
        by_operation.append({
            "operation": r[0],
            "total": r[1],
            "successes": r[2],
            "failures": r[3],
            "success_rate": round(r[2] / r[1] * 100, 1) if r[1] else 0,
            "avg_duration_ms": int(r[4] or 0),
        })

    return {
        "total": total_row[0],
        "successes": total_row[1] or 0,
        "failures": (total_row[0] or 0) - (total_row[1] or 0),
        "avg_duration_ms": int(total_row[2] or 0),
        "by_operation": by_operation,
    }


if __name__ == "__main__":
    import uvicorn
    from updater import start_update_checker

    print(f"ServiceBox Proxy v{VERSION}")

    # Check for updates — if an update is applied, the process restarts
    if start_update_checker():
        print("Mise a jour en cours, arret...")
        sys.exit(0)

    uvicorn.run(app, host="0.0.0.0", port=3847)
