#!/usr/bin/env python3
"""
ServiceBox Local Proxy
Runs on the garage PC (on Stellantis VPN) to bridge the frontend
with ServiceBox + Alpha DMS.
"""

import base64
import re
import json
import random
import sys
import time
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

app = FastAPI(title="ServiceBox Proxy", version=VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── In-memory log ring buffer ────────────────────────────────────────────

_log_entries: deque = deque(maxlen=200)


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
    # Intervention
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
    error: Optional[str] = None
    steps: list[StepResult] = []


class AgendaOptionsResponse(BaseModel):
    receptionnaires: list[dict]  # [{id, name}]
    equipes: list[dict]          # [{id, name}]


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

        # Step 1: Probe without auth to discover WWW-Authenticate scheme
        _log("info", "Probe sans auth pour decouvrir le schema d'authentification...", "bootstrap")
        try:
            probe = requests.get(login_url, verify=False, timeout=15, allow_redirects=False,
                                 headers={"User-Agent": self.session.headers["User-Agent"]})
            www_auth = probe.headers.get("WWW-Authenticate", "")
            _log("info", f"Probe: HTTP {probe.status_code}, WWW-Authenticate: '{www_auth}'", "bootstrap")
            all_headers = dict(probe.headers)
            _log("info", f"Probe headers: {json.dumps(all_headers, default=str)}", "bootstrap")
        except Exception as e:
            _log("warn", f"Probe echouee: {e}", "bootstrap")
            www_auth = ""

        # Step 2: Choose auth strategy based on WWW-Authenticate header
        www_auth_lower = www_auth.lower()
        if "ntlm" in www_auth_lower or "negotiate" in www_auth_lower:
            _log("info", f"Schema detecte: NTLM/Negotiate — utilisation de HttpNtlmAuth", "bootstrap")
            self.session.auth = HttpNtlmAuth(self.user, self.password)
            self.auth_method = "ntlm"
        else:
            _log("info", f"Schema detecte: Basic — utilisation de Basic Auth header", "bootstrap")
            auth_str = f"{self.user}:{self.password}"
            self.session.headers["Authorization"] = f"Basic {base64.b64encode(auth_str.encode()).decode()}"
            self.auth_method = "basic"

        # Step 3: Actual bootstrap request with chosen auth
        resp = self.session.get(login_url, timeout=30)
        _log("info", f"Bootstrap: HTTP {resp.status_code} ({len(resp.text)} bytes, cookies={list(self.session.cookies.keys())})", "bootstrap")
        _log("info", f"Auth method: {self.auth_method}", "bootstrap")

        # If Basic failed with 401, try NTLM as fallback
        if resp.status_code == 401 and self.auth_method == "basic":
            _log("info", "Basic Auth echoue (401), tentative NTLM en fallback...", "bootstrap")
            self.session.headers.pop("Authorization", None)
            self.session.auth = HttpNtlmAuth(self.user, self.password)
            self.auth_method = "ntlm"
            resp = self.session.get(login_url, timeout=30)
            _log("info", f"Bootstrap NTLM: HTTP {resp.status_code} ({len(resp.text)} bytes, cookies={list(self.session.cookies.keys())})", "bootstrap")

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

    def create_rdv(self, req: CreateRdvRequest) -> CreateRdvResponse:
        """Create RDV + transfer to Alpha DMS. Returns OR number with step tracking."""
        steps: list[StepResult] = []

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

        # Step 3: Save RDV
        _log("info", "Sauvegarde du RDV...", "sauvegarderRdv")
        reception_dt = f"{req.date}{req.heure}"
        restitution_dt = f"{req.date}{req.restitution_heure}"

        save_headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": self.base_url,
            "Referer": f"{self.base_url}/agenda/planningReceptionnaire.action?jbnRedirect=true",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
        }

        payload = self._build_rdv_payload(req, reception_dt, restitution_dt)
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
            result = response.json()
            if result.get("statut") == "error":
                errors = result.get("data", {})
                msgs = []
                for champ in errors.get("champs", []):
                    msgs.append(f"[{champ['nom']}]: {', '.join(champ.get('detail', []))}")
                for msg in errors.get("globales", []):
                    msgs.append(msg)
                detail = "; ".join(msgs)
                _log("error", detail, "sauvegarderRdv")
                steps.append(StepResult(name="Sauvegarde RDV", status="error", detail=detail))
                return CreateRdvResponse(success=False, error=detail, steps=steps)

            retour = result.get("data", {}).get("retour", {})
            dossier_id = retour.get("diInformations", {}).get("dossierId", "")
            _log("info", f"OK — dossierId={dossier_id}", "sauvegarderRdv")
            steps.append(StepResult(name="Sauvegarde RDV", status="ok", detail=f"Dossier {dossier_id}"))

            if not dossier_id:
                steps.append(StepResult(name="Transfert Alpha", status="skipped", detail="Pas de dossierId"))
                return CreateRdvResponse(success=True, error="RDV cree mais pas de dossierId pour le transfert Alpha", steps=steps)

            # Transfer to Alpha DMS
            alpha_steps = self._transfer_to_alpha(dossier_id)
            steps.extend(alpha_steps)

            or_number = None
            last = alpha_steps[-1] if alpha_steps else None
            if last and last.status == "ok" and last.name == "Reponse DMS":
                or_number = last.detail.replace("OR n°", "").strip() if last.detail.startswith("OR") else None

            return CreateRdvResponse(success=True, or_number=or_number, dossier_id=dossier_id, steps=steps)

        except (ValueError, KeyError) as e:
            _log("error", str(e), "sauvegarderRdv")
            steps.append(StepResult(name="Sauvegarde RDV", status="error", detail=str(e)))
            return CreateRdvResponse(success=False, error=f"Erreur parsing reponse: {e}", steps=steps)

    def _transfer_to_alpha(self, dossier_id: str) -> list[StepResult]:
        """Set basket + dmsPutDossier + RelaisServlet + dmsResponse. Returns step results."""
        steps: list[StepResult] = []

        # Step 4: Set basket
        _log("info", f"Panier → dossier {dossier_id}...", "panier")
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

        resp = self.session.post(
            f"{self.base_url}/panier/dmsResponse.do",
            data=dms_payload,
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

    def _build_rdv_payload(self, req: CreateRdvRequest, reception_dt: str, restitution_dt: str) -> list:
        return [
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
            ("clientDto.idDms", ""),
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
            # Work
            ("rdvTravailDtoList[0].id", ""),
            ("rdvTravailDtoList[0].travailId", ""),
            ("rdvTravailDtoList[0].numLdt", "_0"),
            ("rdvTravailDtoList[0].statut", ""),
            ("rdvTravailDtoList[0].idExterne", ""),
            ("rdvTravailDtoList[0].type", ""),
            ("rdvTravailDtoList[0].codeSagai", ""),
            ("rdvTravailDtoList[0].categorie", ""),
            ("rdvTravailDtoList[0].dureeReference", ""),
            ("rdvTravailDtoList[0].ldtDateModificationTimestamp", ""),
            ("rdvTravailDtoList[0].initial", "true"),
            ("rdvTravailDtoList[0].nom", req.travail_nom),
            ("rdvTravailDtoList[0].duree", req.travail_duree),
            ("rdvTravailDtoList[0].equipeId", req.equipe_id),
            ("rdvInterventionDtoList[0].equipeId", req.equipe_id),
            ("rdvInterventionDtoList[0].statut", ""),
            ("rdvInterventionDtoList[0].dureeEstimee", req.travail_duree),
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
        ]


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


# ─── Helper to get or create session ─────────────────────────────────────

def _get_session(creds: Credentials) -> ServiceBoxSession:
    key = creds.username
    if key not in _sessions:
        s = ServiceBoxSession(creds.username, creds.password)
        s.bootstrap()
        _sessions[key] = s
    return _sessions[key]


# ─── Routes ──────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "version": VERSION, "sessions": len(_sessions)}


@app.get("/logs")
def get_logs(limit: int = 50):
    """Return the last N log entries (newest first)."""
    entries = list(_log_entries)[-limit:]
    entries.reverse()
    return {"logs": entries}


@app.post("/test-connection", response_model=TestConnectionResponse)
def test_connection(creds: Credentials):
    """Test that we can reach ServiceBox and authenticate."""
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

    return result


@app.post("/options", response_model=AgendaOptionsResponse)
def get_options(req: AgendaOptionsRequest):
    try:
        session = _get_session(req)
        return session.get_agenda_options()
    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@app.post("/create-rdv", response_model=CreateRdvResponse)
def create_rdv(req: CreateRdvRequest):
    try:
        session = _get_session(req)
        return session.create_rdv(req)
    except Exception as e:
        traceback.print_exc()
        return CreateRdvResponse(success=False, error=str(e))


@app.post("/reset-session")
def reset_session(creds: Credentials):
    """Force re-bootstrap (useful if session expired)."""
    key = creds.username
    _sessions.pop(key, None)
    session = _get_session(creds)
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    from updater import start_update_checker

    print(f"ServiceBox Proxy v{VERSION}")

    # Check for updates — if an update is applied, the process restarts
    if start_update_checker():
        print("Mise a jour en cours, arret...")
        sys.exit(0)

    uvicorn.run(app, host="0.0.0.0", port=3847)
