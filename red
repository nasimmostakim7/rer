"""
Titan Anti-Detect Engine  v8
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Base: Working v7 (canvas 100% preserved exactly)
New in v8:
  • Cross-device login: claim_license() + active_device in Firebase
  • After approved: hide name/phone/login fields, show only code+remaining
  • Android Screen / Full Screen toggle (mobile mode)
  • Touch emulation: mouse clicks detected as touch
  • Collect Cookie as toggle switch button
  • "Wait for Approval" after submit/renew
  • Expired → "Send Renew Request" button, after click → "Wait for Approval"
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import sys, os, random, requests, json, shutil, threading, time, tempfile, hashlib, socket
import multiprocessing
import uuid as _uuid
from datetime import datetime, timezone, timedelta

try:
    import distutils
except ImportError:
    import setuptools.distutils
    sys.modules["distutils"] = setuptools.distutils

import undetected_chromedriver as uc
from selenium_stealth import stealth
from fake_useragent import UserAgent
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QRadioButton, QLabel, QListWidget, QTextEdit,
    QMessageBox, QGroupBox, QSpinBox, QListWidgetItem, QAbstractItemView,
    QInputDialog, QCheckBox, QLineEdit, QDialog,
    QComboBox, QFrame, QScrollArea,
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer
from PyQt6.QtGui import QFont

try:
    import firebase_admin
    from firebase_admin import credentials as fb_creds, firestore as fb_store
    FIREBASE_AVAILABLE = True
except ImportError:
    FIREBASE_AVAILABLE = False

# ── EXE-safe base directory ───────────────────────────────────────
# When running as a Nuitka/PyInstaller EXE, sys.executable points to
# the EXE itself. Using its directory ensures all data files (profiles,
# DB, session) are written next to the EXE, not inside a temp folder.
if getattr(sys, "frozen", False):
    # PyInstaller onefile
    BASE_DIR = os.path.dirname(sys.executable)
elif "__compiled__" in dir():
    # Nuitka onefile
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PROFILES_DIR  = os.path.join(BASE_DIR, "titan_profiles")
DB_FILE       = os.path.join(BASE_DIR, "titan_db.json")
SESSION_FILE  = os.path.join(BASE_DIR, "titan_session.json")
_CHROME_LAUNCH_LOCK = threading.Lock()

# ══════════════════════════════════════════════════════════════════
#  FIREBASE  (PEM-safe single-line key)
# ══════════════════════════════════════════════════════════════════

_PRIVATE_KEY_JSON = (
    "-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEA"
    "AoIBAQDAT2Od6ZKnnBlb\\n95ilInOemp9eRzJtkmXRymmynO/JSXUePZ8DuR1V/ZHWdLcSCV"
    "biejS6zFdfToV9\\n0tX3bj34vdvUa80RF9sXfp/SvC8+t8lIvWSOtKiCxfPmDkEvaiX7NgmK"
    "EglrhZoD\\nmhe4JusSgNgH0x7Udp9Bs8HhSHzF9+UMg4WxOudfbdTucV5QCsH91whMEOV25K"
    "Ly\\nWsvgk8N2DkpurvPtlXZExJB6Vy8kuouUxkmasv/q8QOmFjIOwpaXrUKTp2c1dvDO\\nyV"
    "OOyRBCHG0JuzX6Z5K6BmDwrv1q0pQI39GCa0qXQWtMP6ZFdxYT7C+cnMKBuRBe\\njs7GquNJ"
    "AgMBAAECggEABWyX0jXq2YhpHLN0nj/FBBW3ZgjbDsUg0xSC+7M6fjJw\\nt7wkNo4rmifPS7"
    "26Biu2RCgPQa+OzVThJgXi3BpDbDcqsqihvZgvKU/8gQymxn2X\\nk1IoOnA1Co87zGLditFT"
    "Je1Fwpu0oVBA4lYf0iVi3wA8V6goR/TalBdWYhzRzCGf\\nx3HZwKDCMxcckxEQ0xXYtCKRVh"
    "MJG929P4lsiMnyUlzgMZTh25VE2gK771g94/Qe\\n6gRf9oAcSxQjs1O7mb2EZOy3lAOVDof1"
    "XR7G7dEIDtTeG5wq08f+pCz3GoKHqUOx\\nuSiFA1hUoW2odOUiB/IocU7kWYkQxqWmJXK/ko"
    "x5IwKBgQDx+Mz43YVWcRHBP4N+\\n8RQJaf2fxVQHFBQ85h6tBeUOQ3tZgtC74pg/CbAelFUe"
    "HLKls4zR0R2o7z79i8QH\\nEjxop+XARLOzQWGlFI2hSSQACQoEL5mhBCHMV6n7CN85CVD9gV"
    "cFm057g56lO/WM\\nRcM64CvHqmUSyIfG90iK0c4mMwKBgQDLdYntxf9EXd7tgSN7xnY7G4hT"
    "dMBxrzZz\\nTf57eZpnUq0tYOHT/PvovLH8TxkXuJra2cZ0RfAUaYcfUBCIimQ7CDUfvYO5M0"
    "1U\\ncQV4cHGhqDiSeLvwPAb66vebgT2iwZHLCa6UqevbFuttj4zLiG+Yhgm3CM2g8dh5\\nPQ"
    "ymhU08kwKBgQDPTm2tuXwHNxATFKtAEqMr/ZbBT3pSJi3Ajxcw/Z/kvIPtiFn0\\n3om5WD9/"
    "s23JQqT7ufynthVHKtI4v4nO1RzUPSRluXaL2TQjDpzY1aT2MshWFcH1\\nZWjffSuwW0WDxC"
    "uTRUCdGRYAVB+TSO9yokJFKtHXWnEFyrApEqsf4+hOaQKBgFYV\\noEtTeAMkOJuDBVFskj/G"
    "EXNGNdqkCMTWnjL+K59F8vH9SO+Z3bgGhsQ7b2GDATpR\\n5E7z/HWWhM5x4Nz0uR3lBh0s4V"
    "vt4e01eNwRr3J3q6AFp3co3scxvZw1HbAMeLRQ\\nn2ZVUu67DtenYioHyzfclqWz+tT7Ht2FQ"
    "CIAysIzAoGABcnsKLBI5wiH/IiHW0v6\\nO3Mj8g5D8a7x0ovIKAieTi5veR50vfK9ckSZE5A"
    "br/9aforxcVcAO1ko0gEfM4/j\\nFLldDmg+MVg330Rm29WGpL9KZlGMyeJNXq5qFQL2seb8BI"
    "MaUvmLcJDPF1jaCv3k\\nn3TllcJ9Mgnic5zPrerhcWM=\\n-----END PRIVATE KEY-----\\n"
)

_SA_RAW = {
    "type": "service_account",
    "project_id": "tian-19bcf",
    "private_key_id": "57612d2429ce0daeb59b526bf4cdb41a1bc78987",
    "private_key": _PRIVATE_KEY_JSON,
    "client_email": "firebase-adminsdk-fbsvc@tian-19bcf.iam.gserviceaccount.com",
    "client_id":    "106744689960278331826",
    "auth_uri":     "https://accounts.google.com/o/oauth2/auth",
    "token_uri":    "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": (
        "https://www.googleapis.com/robot/v1/metadata/x509/"
        "firebase-adminsdk-fbsvc%40tian-19bcf.iam.gserviceaccount.com"
    ),
    "universe_domain": "googleapis.com",
}

def _get_clean_sa() -> dict:
    sa = dict(_SA_RAW)
    pk = sa.get("private_key", "")
    pk = pk.replace("\\n", "\n")
    if "-----BEGIN PRIVATE KEY-----" not in pk:
        raise ValueError("[Titan] Private key malformed.")
    sa["private_key"] = pk
    return sa

_db = None
def get_db():
    global _db
    if _db is not None: return _db
    if not FIREBASE_AVAILABLE: return None
    try:
        if not firebase_admin._apps:
            firebase_admin.initialize_app(fb_creds.Certificate(_get_clean_sa()))
        _db = fb_store.client()
        return _db
    except Exception as e:
        print(f"[Titan] Firebase: {e}")
        return None

# ══════════════════════════════════════════════════════════════════
#  DEVICE CODE (HWID)
# ══════════════════════════════════════════════════════════════════

def get_device_code() -> str:
    try:
        return hashlib.sha256(
            f"titan-{socket.gethostname()}-{_uuid.getnode()}".encode()
        ).hexdigest()[:10].upper()
    except Exception:
        return "UNKNOWN0000"

DEVICE_CODE = get_device_code()

# ══════════════════════════════════════════════════════════════════
#  SESSION (cross-device login tracking)
# ══════════════════════════════════════════════════════════════════

def get_session_code() -> str:
    """Returns the license code currently active on this device."""
    try:
        with open(SESSION_FILE) as f:
            return json.load(f).get("code", DEVICE_CODE)
    except:
        return DEVICE_CODE

def set_session_code(code: str):
    try:
        with open(SESSION_FILE, "w") as f:
            json.dump({"code": code, "device": DEVICE_CODE}, f)
    except:
        pass

# ══════════════════════════════════════════════════════════════════
#  LOCAL DB — serial numbers
# ══════════════════════════════════════════════════════════════════

def _load_db() -> dict:
    try:
        with open(DB_FILE) as f: return json.load(f)
    except: return {"next_serial": 1, "profiles": {}}

def _save_db(data: dict):
    with open(DB_FILE, "w") as f: json.dump(data, f, indent=2)

def allocate_serial(pid: str) -> int:
    db = _load_db()
    if pid in db.get("profiles", {}): return db["profiles"][pid].get("serial", 0)
    s = db.get("next_serial", 1)
    db.setdefault("profiles", {})[pid] = {"serial": s}
    db["next_serial"] = s + 1
    _save_db(db); return s

def remove_serial(pid: str):
    db = _load_db(); db.get("profiles", {}).pop(pid, None); _save_db(db)

# ══════════════════════════════════════════════════════════════════
#  LICENSE FUNCTIONS
# ══════════════════════════════════════════════════════════════════

def check_license(code: str = "") -> dict:
    """
    Check license.
    - No code: uses session code (supports cross-device login)
    - With code: checks that specific document
    Also enforces active_device — only one device can use a license at a time.
    """
    if not code.strip():
        code = get_session_code()
    target = code.strip().upper()
    db = get_db()
    if db is None:
        return {"status": "no_firebase", "remaining_days": 0, "name": "", "code": target}
    try:
        doc = db.collection("users").document(target).get()
        if not doc.exists:
            return {"status": "not_found", "remaining_days": 0, "name": "", "code": target}
        d      = doc.to_dict()
        status = d.get("status", "pending")
        name   = d.get("name", "")
        now    = datetime.now(timezone.utc)

        if d.get("is_blocked", False):
            rem = max(0, int(d.get("remaining_seconds_at_block", 0) / 86400))
            return {"status": "blocked", "remaining_days": rem, "name": name, "code": target}

        if status == "approved":
            exp = d.get("expires_at")
            if exp:
                rem_secs = (exp - now).total_seconds() if hasattr(exp, "timestamp") else 0
                rem_days = max(0, int(rem_secs / 86400))
                if rem_days <= 0:
                    return {"status": "expired", "remaining_days": 0, "name": name, "code": target}
                # Cross-device check: active_device must be this device
                active_dev = d.get("active_device", "")
                if active_dev and active_dev != DEVICE_CODE:
                    return {"status": "device_mismatch", "remaining_days": rem_days,
                            "name": name, "code": target,
                            "active_device": active_dev}
                return {"status": "approved", "remaining_days": rem_days,
                        "name": name, "code": target,
                        "subscription_days": d.get("subscription_days", 30),
                        "renew_auto_sent": d.get("renew_auto_sent", False)}

        return {"status": status, "remaining_days": 0, "name": name, "code": target}
    except Exception as e:
        return {"status": "error", "remaining_days": 0, "name": "", "code": target, "err": str(e)}


def claim_license(code: str) -> tuple[bool, str]:
    """
    Login by code on a different device.
    Updates active_device to DEVICE_CODE so only this machine can use the license.
    Previous device gets 'device_mismatch' on next check.
    """
    db = get_db()
    if db is None: return False, "Firebase unavailable"
    code = code.strip().upper()
    try:
        doc = db.collection("users").document(code).get()
        if not doc.exists: return False, "Code not found"
        d = doc.to_dict()
        if d.get("is_blocked", False): return False, "This license is blocked"
        if d.get("status","") != "approved": return False, f"License not active (status: {d.get('status','')})"
        exp = d.get("expires_at")
        if exp:
            now = datetime.now(timezone.utc)
            if hasattr(exp, "timestamp") and (exp - now).total_seconds() <= 0:
                return False, "License has expired"
        db.collection("users").document(code).update({
            "active_device": DEVICE_CODE,
            "last_login_at": fb_store.SERVER_TIMESTAMP,
        })
        set_session_code(code)
        return True, ""
    except Exception as e:
        return False, str(e)


def submit_license_request(name: str, phone: str) -> bool:
    db = get_db()
    if db is None: return False
    try:
        db.collection("users").document(DEVICE_CODE).set({
            "device_code":   DEVICE_CODE,
            "name":          name,
            "phone":         phone,
            "status":        "pending",
            "is_blocked":    False,
            "active_device": DEVICE_CODE,
            "requested_at":  fb_store.SERVER_TIMESTAMP,
        }, merge=True)
        set_session_code(DEVICE_CODE)
        return True
    except Exception:
        return False


def submit_renew_request() -> bool:
    db = get_db()
    if db is None: return False
    try:
        code = get_session_code()
        db.collection("users").document(code).update({
            "status": "pending",
            "renew_requested_at": fb_store.SERVER_TIMESTAMP,
        })
        return True
    except Exception:
        return False

# ══════════════════════════════════════════════════════════════════
#  LANGUAGE MAP
# ══════════════════════════════════════════════════════════════════

_COUNTRY_LANG = {
    "US":["en-US","en"],"GB":["en-GB","en"],"CA":["en-CA","en"],
    "AU":["en-AU","en"],"BD":["bn-BD","bn","en"],"IN":["hi-IN","hi","en"],
    "PK":["ur-PK","ur","en"],"DE":["de-DE","de","en"],"FR":["fr-FR","fr","en"],
    "ES":["es-ES","es","en"],"MX":["es-MX","es","en"],"BR":["pt-BR","pt","en"],
    "JP":["ja-JP","ja","en"],"KR":["ko-KR","ko","en"],"CN":["zh-CN","zh","en"],
    "RU":["ru-RU","ru","en"],"TR":["tr-TR","tr","en"],
    "SA":["ar-SA","ar","en"],"AE":["ar-AE","ar","en"],"EG":["ar-EG","ar","en"],
    "ID":["id-ID","id","en"],"TH":["th-TH","th","en"],"VN":["vi-VN","vi","en"],
    "NL":["nl-NL","nl","en"],"IT":["it-IT","it","en"],"SE":["sv-SE","sv","en"],
    "PL":["pl-PL","pl","en"],"SG":["en-SG","en"],"NG":["en-NG","en"],
}

def get_lang_for_ip(ip: str = "") -> list:
    try:
        url  = f"http://ip-api.com/json/{ip}" if ip else "http://ip-api.com/json/"
        data = requests.get(url, timeout=5).json()
        return _COUNTRY_LANG.get(data.get("countryCode","US"), ["en-US","en"])
    except: return ["en-US","en"]

# ══════════════════════════════════════════════════════════════════
#  PROXY PARSING + CHROME EXTENSION AUTH
# ══════════════════════════════════════════════════════════════════

def parse_proxy(raw: str) -> dict:
    raw = raw.strip(); scheme = "http"
    if "://" in raw: scheme, raw = raw.split("://",1); scheme = scheme.lower()
    parts = raw.split(":")
    if len(parts) == 2: return {"host":parts[0],"port":parts[1],"user":"","pass":"","scheme":scheme}
    if len(parts) == 3: return {"host":parts[0],"port":parts[1],"user":parts[2],"pass":"","scheme":scheme}
    if len(parts) >= 4: return {"host":parts[0],"port":parts[1],"user":parts[2],"pass":":".join(parts[3:]),"scheme":scheme}
    return {"host":raw,"port":"8080","user":"","pass":"","scheme":scheme}

def build_proxy_extension(host,port,user,pwd,scheme="http") -> str:
    manifest = {"version":"1.0.0","manifest_version":2,"name":"Titan Proxy Auth",
                "permissions":["proxy","tabs","unlimitedStorage","storage","<all_urls>","webRequest","webRequestBlocking"],
                "background":{"scripts":["background.js"]},"minimum_chrome_version":"22.0.0"}
    bg = f"""
var config={{mode:"fixed_servers",rules:{{singleProxy:{{scheme:"{scheme}",host:"{host}",port:parseInt("{port}")}},bypassList:["localhost","127.0.0.1"]}}}};
chrome.proxy.settings.set({{value:config,scope:"regular"}},function(){{}});
chrome.webRequest.onAuthRequired.addListener(function(d){{return{{authCredentials:{{username:"{user}",password:"{pwd}"}}}}}},{{urls:["<all_urls>"]}},["blocking"]);
"""
    d = tempfile.mkdtemp(prefix="titan_px_")
    with open(os.path.join(d,"manifest.json"),"w") as f: json.dump(manifest,f,indent=2)
    with open(os.path.join(d,"background.js"),"w") as f: f.write(bg)
    return d

def validate_proxy(raw: str) -> tuple[bool, str]:
    if not raw: return True,""
    p = parse_proxy(raw)
    try:
        port = int(p["port"])
        if not (1 <= port <= 65535): raise ValueError
    except ValueError: return False,f"Invalid port: '{p['port']}'"
    test = (f"{p['scheme']}://{p['user']}:{p['pass']}@{p['host']}:{p['port']}"
            if p["user"] and p["pass"] else f"{p['scheme']}://{p['host']}:{p['port']}")
    try:
        requests.get("http://ip-api.com/json/",proxies={"http":test,"https":test},timeout=8)
        return True,""
    except Exception as e:
        return False,f"Proxy unreachable: '{p['host']}:{p['port']}'\n{str(e)[:150]}"

# ══════════════════════════════════════════════════════════════════
#  FINGERPRINT
# ══════════════════════════════════════════════════════════════════

def generate_mac():
    m = [random.randint(0,255) for _ in range(6)]
    m[0] = (m[0]&0xFE)|0x02
    return ":".join(f"{b:02x}" for b in m)

def fake_lan_ip():
    p = random.choice(["192.168","10.0","10.10","172.16"])
    return f"{p}.{random.randint(1,254)}.{random.randint(2,253)}"

def _gen_battery() -> dict:
    """
    Generate 100% unique, dynamic, realistic battery state per profile.
    Produces: level, charging, chargingTime, dischargingTime,
              drain_rate_secs_per_pct, charge_rate_secs_per_pct
    The JS uses drain/charge rates to simulate live battery changes.
    """
    charging = random.choice([True, False])
    if charging:
        # Phone is plugged in — level can be anywhere 0.30–0.97
        level = round(random.uniform(0.30, 0.97), 2)
        # Charging time: seconds to reach 100%
        # Realistic: ~1% per 40–90 seconds while charging fast
        charge_rate = random.randint(40, 90)   # sec / 1%
        charging_time = int((1.0 - level) * 100 * charge_rate)
        discharging_time = "Infinity"
        charge_rate_js  = charge_rate
        drain_rate_js   = random.randint(200, 500)   # stored but not used while charging
    else:
        # Phone is on battery — level 0.10–0.92, not plugged
        level = round(random.uniform(0.10, 0.92), 2)
        # Discharging time: seconds to reach 0%
        # Realistic: ~1% per 200–550 seconds (≈ 3–9 h battery life)
        drain_rate = random.randint(200, 550)  # sec / 1%
        discharging_time = int(level * 100 * drain_rate)
        charging_time = "Infinity"
        drain_rate_js   = drain_rate
        charge_rate_js  = random.randint(40, 90)   # stored but not used while discharging

    return {
        "battery_level":           level,
        "battery_charging":        charging,
        "battery_charging_time":   charging_time,
        "battery_discharging_time":discharging_time,
        "battery_drain_rate":      drain_rate_js,
        "battery_charge_rate":     charge_rate_js,
    }

def resolve_timezone_for_proxy(proxy: str) -> str:
    """
    Detect the real timezone of the current external IP.
    If proxy is set, queries through the proxy so we get the proxy's
    geo-location (not the local machine's).  Falls back gracefully.
    """
    try:
        if proxy:
            p = parse_proxy(proxy)
            test_url = (f"{p['scheme']}://{p['user']}:{p['pass']}@{p['host']}:{p['port']}"
                        if p["user"] and p["pass"] else
                        f"{p['scheme']}://{p['host']}:{p['port']}")
            data = requests.get("http://ip-api.com/json/",
                                proxies={"http": test_url, "https": test_url},
                                timeout=6).json()
        else:
            data = requests.get("http://ip-api.com/json/", timeout=5).json()
        tz = data.get("timezone", "")
        return tz if tz else "America/New_York"
    except Exception:
        return "America/New_York"

def build_fingerprint(device_mode: str, lang_override=None) -> dict:
    ua_gen = UserAgent()
    actual = random.choice(["desktop","mobile"]) if device_mode=="mixed" else device_mode
    desktop_gpus=[("NVIDIA Corporation","GeForce RTX 4060/PCIe/SSE2"),
                  ("NVIDIA Corporation","GeForce RTX 3070/PCIe/SSE2"),
                  ("Intel Inc.","Intel(R) UHD Graphics 770"),
                  ("Intel Inc.","Intel(R) Iris Xe Graphics"),
                  ("AMD","Radeon RX 7600/SSE2"),("AMD","Radeon RX 6700 XT/SSE2")]
    mobile_gpus=[("Qualcomm","Adreno (TM) 740"),("ARM","Mali-G715-MC11"),
                 ("Imagination Technologies","PowerVR GE8320")]
    if actual=="mobile":
        devs=[
            {"name":"Pixel 7","w":412,"h":915,"dpr":2.625,"ua":"Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36"},
            {"name":"Samsung Galaxy S23","w":360,"h":780,"dpr":3.0,"ua":"Mozilla/5.0 (Linux; Android 13; SM-S911B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36"},
            {"name":"OnePlus 11","w":412,"h":919,"dpr":2.0,"ua":"Mozilla/5.0 (Linux; Android 13; CPH2449) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36"},
            {"name":"Xiaomi 13","w":393,"h":851,"dpr":2.75,"ua":"Mozilla/5.0 (Linux; Android 13; 2211133C) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36"},
        ]
        device=random.choice(devs); gpu_v,gpu_r=random.choice(mobile_gpus); platform="Linux armv8l"
    else:
        bases=[{"name":"Windows Desktop","platform":"Win32","w":1920,"h":1080,"dpr":1.0},
               {"name":"Windows Laptop","platform":"Win32","w":1366,"h":768,"dpr":1.25},
               {"name":"MacBook Pro 16","platform":"MacIntel","w":1440,"h":900,"dpr":2.0},
               {"name":"iMac 27","platform":"MacIntel","w":2560,"h":1440,"dpr":2.0},
               {"name":"Linux Desktop","platform":"Linux x86_64","w":1920,"h":1080,"dpr":1.0}]
        base=random.choice(bases); platform=base["platform"]
        device={**base,"w":base["w"]+random.randint(-50,50),"h":base["h"]+random.randint(-10,10),"ua":ua_gen.chrome}
        gpu_v,gpu_r=random.choice(desktop_gpus)
    langs=(lang_override if lang_override else random.choice([
        ["en-US","en"],["en-GB","en"],["de-DE","de","en"],["fr-FR","fr","en"],
        ["es-ES","es","en"],["pt-BR","pt","en"],["ja-JP","ja","en"],["ko-KR","ko","en"]]))
    nt=[random.randint(1,255) for _ in range(16)]
    cr,cg,cb=random.randint(0,255),random.randint(0,255),random.randint(0,255)
    return {
        "actual_mode":actual,"device":device,"platform":platform,
        "gpu_vendor":gpu_v,"gpu_renderer":gpu_r,
        "cores":random.choice([2,4,6,8,12,16]),"ram":random.choice([4,8,16,32]),
        "languages":langs,"mac":generate_mac(),"noise_table":nt,
        "canvas_seed":random.randint(100,65000),"canvas_rgb":[cr,cg,cb],
        "canvas_alpha":round(random.uniform(0.01,0.06),4),
        "audio_noise":round(random.uniform(0.00002,0.0001),6),
        # ── Realistic dynamic battery masking ──────────────────────
        # Each profile gets a unique, realistic battery state.
        # charging=True  → level between 0.30–0.95 (mid-charge range)
        # charging=False → level between 0.10–0.92 (draining range)
        # drain_rate: seconds to drain 1% (realistic: 200–600 s/%)
        # charge_rate: seconds to charge 1% (realistic: 40–120 s/%)
        # These make the JS-simulated live drain/charge look real.
        **_gen_battery(),
        "fake_lan":fake_lan_ip(),"font_noise":round(random.uniform(0.0001,0.0005),6),
        "font_shift":round(random.uniform(0.02,0.12),4),
        "rtt":random.randint(15,80),"downlink":round(random.uniform(5,50),1),
    }

# ══════════════════════════════════════════════════════════════════
#  UTILITY
# ══════════════════════════════════════════════════════════════════

def ip_to_timezone(ip):
    try: return requests.get(f"http://ip-api.com/json/{ip}",timeout=4).json().get("timezone","America/New_York")
    except: return "America/New_York"

def my_public_timezone():
    try: return requests.get("http://ip-api.com/json/",timeout=4).json().get("timezone","America/New_York")
    except: return "America/New_York"

def load_meta(pid):
    mp=os.path.join(PROFILES_DIR,pid,"titan_meta.json")
    try:
        with open(mp) as f: return json.load(f)
    except: return {}

def save_meta(pid,fp,proxy,tz,name="",serial=0,lang_mode="default",
              webrtc_mode="mask",collect_cookie=False,android_display="android_screen"):
    pdir=os.path.join(PROFILES_DIR,pid); os.makedirs(pdir,exist_ok=True)
    with open(os.path.join(pdir,"titan_meta.json"),"w") as f:
        json.dump({
            "id":pid,"serial":serial,"profile_name":name,"mode":fp["actual_mode"],
            "proxy":proxy,"lang_mode":lang_mode,"languages":fp["languages"],
            "timezone":tz,"mac":fp["mac"],"device":fp["device"].get("name","?"),
            "fake_lan":fp["fake_lan"],"gpu":f'{fp["gpu_vendor"]}/{fp["gpu_renderer"]}',
            "cores":fp["cores"],"ram":fp["ram"],
            "webrtc_mode":webrtc_mode,"collect_cookie":collect_cookie,
            "android_display":android_display,
            "fingerprint":fp,
        },f,indent=2)

def update_meta_name(pid,new_name):
    mp=os.path.join(PROFILES_DIR,pid,"titan_meta.json")
    try:
        with open(mp) as f: data=json.load(f)
        data["profile_name"]=new_name
        with open(mp,"w") as f: json.dump(data,f,indent=2)
    except: pass

def update_meta_cookie(pid: str, enabled: bool):
    """Retroactively enable or disable cookie collection on an existing profile."""
    mp=os.path.join(PROFILES_DIR,pid,"titan_meta.json")
    try:
        with open(mp) as f: data=json.load(f)
        data["collect_cookie"]=enabled
        with open(mp,"w") as f: json.dump(data,f,indent=2)
    except: pass

# ══════════════════════════════════════════════════════════════════
#  BROWSER THREAD
# ══════════════════════════════════════════════════════════════════

def _get_chrome_major_version() -> int | None:
    """
    Bulletproof Chrome version detection.
    Tries 5 methods in order — returns major version int (e.g. 147) or None.
    """
    import subprocess, re, platform
    system = platform.system()

    def _parse(ver_str: str) -> int | None:
        m = re.search(r"(\d{2,3})\.", ver_str)
        return int(m.group(1)) if m else None

    if system == "Windows":
        # Method 1: Registry HKLM (most reliable for standard installs)
        try:
            import winreg
            for hive in (winreg.HKEY_LOCAL_MACHINE, winreg.HKEY_CURRENT_USER):
                for sub in (
                    r"SOFTWARE\Google\Chrome\BLBeacon",
                    r"SOFTWARE\Chromium\BLBeacon",
                    r"SOFTWARE\WOW6432Node\Google\Chrome\BLBeacon",
                    r"SOFTWARE\Google\Update\Clients\{8A69D345-D564-463c-AFF1-A69D9E530F96}",
                ):
                    try:
                        key = winreg.OpenKey(hive, sub)
                        ver, _ = winreg.QueryValueEx(key, "version" if "BLBeacon" in sub else "pv")
                        winreg.CloseKey(key)
                        v = _parse(ver)
                        if v: return v
                    except Exception:
                        pass
        except Exception:
            pass

        # Method 2: Read version from chrome.exe file properties (no subprocess)
        for exe_path in [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            os.path.join(os.environ.get("LOCALAPPDATA",""),
                         r"Google\Chrome\Application\chrome.exe"),
            os.path.join(os.environ.get("PROGRAMFILES",""),
                         r"Google\Chrome\Application\chrome.exe"),
        ]:
            if not os.path.exists(exe_path):
                continue
            try:
                # Read version from the parent directory name (Chrome stores version in folder)
                app_dir = os.path.dirname(exe_path)
                for entry in os.listdir(app_dir):
                    v = _parse(entry)
                    if v and v >= 80:
                        return v
            except Exception:
                pass
            # Fallback: run the exe with --version
            try:
                result = subprocess.run(
                    [exe_path, "--version"],
                    capture_output=True, text=True, timeout=8,
                    creationflags=0x08000000   # CREATE_NO_WINDOW
                )
                v = _parse(result.stdout + result.stderr)
                if v: return v
            except Exception:
                pass

        # Method 3: PowerShell query (works even for non-standard installs)
        try:
            ps_cmd = (
                "(Get-Item (Get-Command chrome.exe).Source).VersionInfo.ProductVersion"
            )
            result = subprocess.run(
                ["powershell", "-NoProfile", "-NonInteractive", "-Command", ps_cmd],
                capture_output=True, text=True, timeout=8,
                creationflags=0x08000000
            )
            v = _parse(result.stdout)
            if v: return v
        except Exception:
            pass

    elif system == "Darwin":
        for exe in [
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            "/Applications/Chromium.app/Contents/MacOS/Chromium",
        ]:
            if os.path.exists(exe):
                try:
                    result = subprocess.run([exe, "--version"], capture_output=True,
                                            text=True, timeout=8)
                    v = _parse(result.stdout)
                    if v: return v
                except Exception:
                    pass

    else:  # Linux
        for exe in ["google-chrome", "google-chrome-stable",
                    "google-chrome-beta", "chromium", "chromium-browser"]:
            try:
                result = subprocess.run([exe, "--version"], capture_output=True,
                                        text=True, timeout=8)
                v = _parse(result.stdout)
                if v: return v
            except Exception:
                pass

    return None   # uc will auto-detect as last resort


# ══════════════════════════════════════════════════════════════════
#  PORTABLE CHROMIUM SUPPORT
# ══════════════════════════════════════════════════════════════════

def _get_chromium_exe() -> str | None:
    """
    Find portable Chromium executable.
    Looks in BASE_DIR/chromium/ (next to the running .exe).
    Priority: chrome.exe → chromium.exe → chrome → chromium
    Returns full path or None (fall back to system Chrome).
    """
    for name in ("chrome.exe", "chromium.exe", "chrome", "chromium"):
        candidate = os.path.join(BASE_DIR, "chromium", name)
        if os.path.isfile(candidate):
            return candidate
    return None


def _get_chromium_version_from_exe(exe_path: str) -> int | None:
    """
    Read major version from a specific Chrome/Chromium exe.
    Tries version-numbered subfolder first (fastest), then subprocess.
    Returns int (e.g. 124) or None.
    """
    import re, subprocess

    # Method 1: Numbered version subfolder  chromium/124.0.6367.82/
    try:
        base = os.path.dirname(exe_path)
        for entry in os.listdir(base):
            m = re.match(r"^(\d{2,3})\.\d+\.\d+\.\d+$", entry)
            if m:
                v = int(m.group(1))
                if 80 <= v <= 999:
                    return v
    except Exception:
        pass

    # Method 2: Run exe with --version
    try:
        import platform
        kw = {}
        if platform.system() == "Windows":
            kw["creationflags"] = 0x08000000   # CREATE_NO_WINDOW
        result = subprocess.run(
            [exe_path, "--version"],
            capture_output=True, text=True, timeout=8, **kw
        )
        m = re.search(r"(\d{2,3})\.", result.stdout + result.stderr)
        if m:
            return int(m.group(1))
    except Exception:
        pass

    return None


def _sync_ua_to_version(ua: str, version: int) -> str:
    """
    Replace Chrome/NNN.x.x.x in User-Agent with the actual Chromium version.
    Prevents sec-ch-ua / UA mismatch detection.
    E.g. Chrome/130.0.0.0 → Chrome/124.0.0.0
    """
    import re
    return re.sub(
        r"Chrome/\d+\.\d+\.\d+\.\d+",
        f"Chrome/{version}.0.0.0",
        ua
    )


def _patch_chromedriver_binary():
    """
    Binary patching: Replace 'cdc_' and '$wdc_' automation strings in
    chromedriver.exe with harmless padding so top-tier detectors can't find them.

    undetected_chromedriver already does this, but calling it explicitly here
    ensures it happens in the locked section before any driver is spawned.
    Only patches once per session (idempotent).
    """
    try:
        import undetected_chromedriver.patcher as _patcher
        p = _patcher.Patcher()
        if not p.is_binary_patched():
            p.patch()
    except Exception:
        pass   # Graceful: if already patched or path not found, continue


def _launch_chromium(opts: "uc.ChromeOptions") -> "uc.Chrome":
    """
    Smart launcher — prefers portable Chromium over system Chrome.

    If BASE_DIR/chromium/chrome.exe exists:
      • Uses it exclusively (no system Chrome touched)
      • Auto-detects its version → pins chromedriver
      • Syncs User-Agent to match (no version fingerprint leak)

    Otherwise falls back to system Chrome with pinned version.
    Caller must already hold _CHROME_LAUNCH_LOCK.
    """
    portable_exe = _get_chromium_exe()

    if portable_exe:
        version = _get_chromium_version_from_exe(portable_exe)
        if version is None:
            raise RuntimeError(
                f"Portable Chromium found at:\n{portable_exe}\n\n"
                "But could not determine its version. "
                "Ensure the chromium folder is not corrupted."
            )
        drv = uc.Chrome(
            options=opts,
            browser_executable_path=portable_exe,
            version_main=version,
        )
    else:
        # No portable — use system Chrome with bulletproof version detection
        version = _get_chrome_major_version()
        drv = uc.Chrome(options=opts, version_main=version)

    return drv, (portable_exe is not None)


_CLOSE_PHRASES=("target frame detached","disconnected","unable to receive message from renderer",
                "no such window","session deleted","chrome not reachable","connection refused",
                "invalid session id","cannot determine loading status")

def _is_close_event(exc): return any(p in str(exc).lower() for p in _CLOSE_PHRASES)


class BrowserThread(QThread):
    launched        = pyqtSignal(str)
    error           = pyqtSignal(str,str)
    browser_closed  = pyqtSignal(str)
    cookies_ready   = pyqtSignal(str, list)

    def __init__(self, pid, meta):
        super().__init__()
        self.pid=pid; self.meta=meta; self._drv=None; self._alive=True; self._ext_dir=None

    def stop(self):
        self._alive=False
        if self._drv:
            try: self._drv.quit()
            except: pass
            self._drv=None

    def run(self):
        try:
            self._drv=self._launch()
        except Exception as e:
            if not _is_close_event(e): self.error.emit(self.pid,str(e))
            self.browser_closed.emit(self.pid); self._cleanup_ext(); return
        self.launched.emit(self.pid)
        while self._alive:
            try: _=self._drv.title; self.msleep(2000)
            except: break
        if self._drv:
            try: self._drv.quit()
            except: pass
            self._drv=None
        self._cleanup_ext(); self.browser_closed.emit(self.pid)

    def _cleanup_ext(self):
        if self._ext_dir and os.path.exists(self._ext_dir):
            shutil.rmtree(self._ext_dir,ignore_errors=True); self._ext_dir=None

    def get_all_cookies(self) -> list:
        """Get cookies from running browser OR from saved disk cache."""
        if self._drv:
            try:
                cookies = self._drv.get_cookies()
                if cookies:
                    # Save to disk so they can be retrieved after browser closes
                    self._save_cookies_to_disk(cookies)
                return cookies
            except Exception:
                pass
        # Browser not running — try disk cache
        return self._load_cookies_from_disk()

    def _cookies_file(self) -> str:
        return os.path.join(PROFILES_DIR, self.pid, "_titan_cookies.json")

    def _save_cookies_to_disk(self, cookies: list):
        try:
            with open(self._cookies_file(), "w", encoding="utf-8") as f:
                json.dump(cookies, f, ensure_ascii=False)
        except Exception: pass

    def _load_cookies_from_disk(self) -> list:
        try:
            cf = self._cookies_file()
            if os.path.exists(cf):
                with open(cf, encoding="utf-8") as f:
                    return json.load(f)
        except Exception: pass
        return []

    def get_cookie_domains(self) -> list:
        cookies = self.get_all_cookies()
        return sorted({c.get("domain","").lstrip(".") for c in cookies if c.get("domain")})

    def get_cookies_for_domain(self, domain: str) -> list:
        cookies = self.get_all_cookies()
        return [c for c in cookies if domain in c.get("domain","")]

    def _launch(self):
        fp=self.meta["fingerprint"]; am=fp["actual_mode"]
        proxy=self.meta.get("proxy",""); webrtc_mode=self.meta.get("webrtc_mode","mask")
        android_display=self.meta.get("android_display","android_screen")
        px_info=parse_proxy(proxy) if proxy else None
        has_auth=px_info and bool(px_info["user"])

        # ── Full proxy connectivity check (safe — runs in BrowserThread) ──
        if proxy:
            ok, err_msg = validate_proxy(proxy)
            if not ok:
                raise RuntimeError(f"Proxy unreachable — {err_msg}")

        opts=uc.ChromeOptions()
        pdir=os.path.join(PROFILES_DIR,self.pid); os.makedirs(pdir,exist_ok=True)
        opts.add_argument(f"--user-data-dir={pdir}")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument("--no-sandbox"); opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-infobars")
        opts.add_argument(f"--user-agent={fp['device']['ua']}")
        opts.add_argument(f"--lang={fp['languages'][0]}")

        if am == "desktop":
            opts.add_argument(f"--window-size={fp['device']['w']},{fp['device']['h']}")
        elif am == "mobile":
            if android_display == "android_screen":
                # ── Android Screen mode: phone-sized portrait window ──
                # Opens a small portrait window matching exact device dimensions.
                # Looks and behaves exactly like holding an Android phone.
                w, h = fp["device"]["w"], fp["device"]["h"]
                opts.add_argument(f"--window-size={w},{h}")
                opts.add_argument(f"--force-device-scale-factor={fp['device']['dpr']}")
                opts.add_argument("--window-position=0,0")
            else:
                # ── Full Screen mode: maximized desktop window, mobile viewport inside ──
                # Window fills monitor; CDP forces mobile resolution/UA internally.
                # This is like GoLogin's "Full Screen" — big window, mobile fingerprint.
                opts.add_argument("--start-maximized")

        if has_auth:
            self._ext_dir=build_proxy_extension(px_info["host"],px_info["port"],
                                                 px_info["user"],px_info["pass"],px_info["scheme"])
            opts.add_argument(f"--load-extension={self._ext_dir}")
        else:
            opts.add_argument("--disable-extensions")
            if px_info: opts.add_argument(f"--proxy-server={px_info['scheme']}://{px_info['host']}:{px_info['port']}")

        if webrtc_mode=="lock":
            opts.add_argument("--disable-webrtc")
            opts.add_argument("--force-webrtc-ip-handling-policy=disable_non_proxied_udp")

        with _CHROME_LAUNCH_LOCK:
            _patch_chromedriver_binary()   # Remove $cdc_ automation strings
            drv, _using_portable = _launch_chromium(opts)
            time.sleep(0.5)

        # ── Version-sync User-Agent when using portable Chromium ──
        # If portable Chromium version differs from the UA in the fingerprint,
        # update it so navigator.userAgent and sec-ch-ua are always consistent.
        if _using_portable:
            _portable_ver = _get_chromium_version_from_exe(_get_chromium_exe())
            if _portable_ver:
                fp["device"]["ua"] = _sync_ua_to_version(fp["device"]["ua"], _portable_ver)
                _ver_str = str(_portable_ver)
                # Patch brands to match real Chromium version
                _brands = [{"brand": "Google Chrome", "version": _ver_str},
                           {"brand": "Chromium",      "version": _ver_str},
                           {"brand": "Not_A Brand",   "version": "99"}]
            else:
                _brands = None
        else:
            _brands = None

        if am=="mobile":
            w, h, dpr = fp["device"]["w"], fp["device"]["h"], fp["device"]["dpr"]

            # Step 1: Force CDP to override all viewport/screen metrics
            # This is the most important step — it overrides what the browser
            # reports to JavaScript, making fingerprints match real Android.
            drv.execute_cdp_cmd("Emulation.setDeviceMetricsOverride", {
                "width":             w,
                "height":            h,
                "deviceScaleFactor": dpr,
                "mobile":            True,
                "hasTouch":          True,
                "screenWidth":       w,
                "screenHeight":      h,
                "positionX":         0,
                "positionY":         0,
                "screenOrientation": {"type": "portraitPrimary", "angle": 0}
            })

            # Step 2: Enable touch — converts mouse clicks → touch events
            drv.execute_cdp_cmd("Emulation.setTouchEmulationEnabled",
                                 {"enabled": True, "maxTouchPoints": 5})

            # Step 3: Override UA with full mobile metadata (version-synced)
            _mob_ver = str(_portable_ver) if (_using_portable and _portable_ver) else "124"
            _mob_full = f"{_mob_ver}.0.0.0"
            drv.execute_cdp_cmd("Network.setUserAgentOverride", {
                "userAgent": fp["device"]["ua"],
                "platform":  fp["platform"],
                "userAgentMetadata": {
                    "brands": _brands if _brands else [
                        {"brand": "Google Chrome", "version": _mob_ver},
                        {"brand": "Chromium",      "version": _mob_ver},
                        {"brand": "Not_A Brand",   "version": "99"},
                    ],
                    "fullVersion":     _mob_full,
                    "platform":        "Android",
                    "platformVersion": "13",
                    "architecture":    "arm",
                    "model":           fp["device"].get("name", "Pixel 7"),
                    "mobile":          True,
                }
            })

            # Step 4: Window sizing per mode.
            # android_screen → small phone-size portrait window (exact device dims)
            # full_screen    → window is maximized, mobile viewport via CDP only
            if android_display == "android_screen":
                # Phone-size window: force exact device dimensions
                try:
                    drv.set_window_rect(x=0, y=0, width=w, height=h)
                except Exception:
                    pass
            # full_screen: window already maximized via --start-maximized
            # Step 5: Inject mouse-wheel → touch scroll translation
            # This makes mousewheel scroll work on touch-only pages.
            drv.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": """
(function patchWheelToTouch(){
  document.addEventListener('wheel', function(e){
    const el = document.elementFromPoint(e.clientX, e.clientY);
    if(!el) return;
    const dy = e.deltaY;
    // Synthesize touchstart → touchmove → touchend to simulate scroll
    const startY = e.clientY;
    const touch = new Touch({identifier: Date.now(), target: el,
      clientX: e.clientX, clientY: startY, radiusX:1, radiusY:1,
      rotationAngle:0, force:1});
    const endTouch = new Touch({identifier: touch.identifier, target: el,
      clientX: e.clientX, clientY: startY - dy, radiusX:1, radiusY:1,
      rotationAngle:0, force:1});
    try {
      el.dispatchEvent(new TouchEvent('touchstart',{touches:[touch],changedTouches:[touch],bubbles:true,cancelable:true}));
      el.dispatchEvent(new TouchEvent('touchmove',{touches:[endTouch],changedTouches:[endTouch],bubbles:true,cancelable:true}));
      el.dispatchEvent(new TouchEvent('touchend',{touches:[],changedTouches:[endTouch],bubbles:true,cancelable:true}));
    } catch(ex){}
  }, {passive:true});
})();
"""})

        # ── Timezone Auto-Sync ────────────────────────────────────
        live_tz = resolve_timezone_for_proxy(proxy)
        drv.execute_cdp_cmd("Emulation.setTimezoneOverride", {"timezoneId": live_tz})
        drv.execute_cdp_cmd("Emulation.setLocaleOverride",{"locale":fp["languages"][0]})
        if am=="desktop":
            ua_override = {"userAgent": fp["device"]["ua"], "platform": fp["platform"]}
            if _brands:
                ua_override["userAgentMetadata"] = {
                    "brands":          _brands,
                    "fullVersion":     f"{_portable_ver}.0.0.0",
                    "platform":        fp["platform"],
                    "platformVersion": "10.0.0",
                    "architecture":    "x86",
                    "model":           "",
                    "mobile":          False,
                }
            drv.execute_cdp_cmd("Network.setUserAgentOverride", ua_override)

        stealth(drv,languages=fp["languages"],vendor="Google Inc.",platform=fp["platform"],
                webgl_vendor=fp["gpu_vendor"],renderer=fp["gpu_renderer"],fix_hairline=True)
        drv.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument",
                            {"source":_build_js(fp,webrtc_mode)})

        # ── Mobile: inject wheel→touch scroll so mouse scroll works ──
        if am == "mobile":
            drv.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": """
(function(){
  // Convert mouse wheel events to touch scroll sequences.
  // This makes mouse wheel scrolling work naturally on mobile pages.
  let _lastY = window.innerHeight / 2;
  document.addEventListener('wheel', function(e) {
    const target = e.target || document.body;
    const startY = _lastY;
    const endY   = _lastY - e.deltaY * 0.5;
    const cx     = e.clientX || window.innerWidth / 2;
    const id     = Date.now();
    const _touch = (y) => ({
      identifier: id, target: target,
      clientX: cx, clientY: y, screenX: cx, screenY: y,
      pageX: cx, pageY: y + window.scrollY,
      radiusX: 11.5, radiusY: 11.5, rotationAngle: 0, force: 1,
    });
    try {
      target.dispatchEvent(new TouchEvent('touchstart', {
        bubbles:true, cancelable:true,
        touches:[new Touch(_touch(startY))],
        targetTouches:[new Touch(_touch(startY))],
        changedTouches:[new Touch(_touch(startY))]
      }));
      target.dispatchEvent(new TouchEvent('touchmove', {
        bubbles:true, cancelable:true,
        touches:[new Touch(_touch(endY))],
        targetTouches:[new Touch(_touch(endY))],
        changedTouches:[new Touch(_touch(endY))]
      }));
      target.dispatchEvent(new TouchEvent('touchend', {
        bubbles:true, cancelable:true,
        touches:[], targetTouches:[],
        changedTouches:[new Touch(_touch(endY))]
      }));
      _lastY = endY;
    } catch(err) {}
  }, {passive: true});
})();
"""})

        # Open blank page — no redirect to external sites
        try: drv.get("about:blank")
        except Exception as e:
            if not _is_close_event(e): raise
        return drv


# ══════════════════════════════════════════════════════════════════
#  JS INJECTION  — EXACT copy from working v7 (canvas 100% preserved)
# ══════════════════════════════════════════════════════════════════

def _build_js(fp: dict, webrtc_mode: str = "mask") -> str:
    am=fp["actual_mode"]; nt=fp["noise_table"]; cs=fp["canvas_seed"]
    cr,cg,cb=fp["canvas_rgb"]; ca=fp["canvas_alpha"]
    cores=fp["cores"]; ram=fp["ram"]
    # ── Dynamic battery values ────────────────────────────────────
    bat        = fp["battery_level"]
    is_chg     = fp["battery_charging"]
    ch         = "true" if is_chg else "false"
    cht        = fp["battery_charging_time"]     # int or "Infinity"
    dis        = fp["battery_discharging_time"]  # int or "Infinity"
    drain_rate = fp["battery_drain_rate"]        # seconds per 1%
    chg_rate   = fp["battery_charge_rate"]       # seconds per 1%
    mac=fp["mac"]; gpuv=fp["gpu_vendor"]; gpur=fp["gpu_renderer"]
    sw,sh=fp["device"]["w"],fp["device"]["h"]; dpr=fp["device"].get("dpr",1.0)
    anoise=fp["audio_noise"]; lj=json.dumps(fp["languages"]); l0=fp["languages"][0]
    pl=fp["platform"]; rtt=fp["rtt"]; dl=fp["downlink"]
    fl=fp["fake_lan"]; fn=fp["font_noise"]; fs=fp["font_shift"]
    mob="true" if am=="mobile" else "false"; nt_js=json.dumps(nt)

    # WebRTC section — depends on mode
    if webrtc_mode == "lock":
        webrtc_js = """
(function lockWebRTC(){
  ['RTCPeerConnection','webkitRTCPeerConnection','mozRTCPeerConnection'].forEach(k=>{
    if(window[k]) Object.defineProperty(window,k,{get:()=>undefined,configurable:true});
  });
})();
"""
    else:
        webrtc_js = f"""
(function maskWebRTC(){{
  if(!window.RTCPeerConnection)return;
  const FAKE='{fl}';
  const swap=s=>s?s.replace(/(\\d{{1,3}}\\.\\d{{1,3}}\\.\\d{{1,3}}\\.\\d{{1,3}})/g,ip=>{{
    const p=ip.split('.').map(Number);return(p[0]===127||p[0]>=224)?ip:FAKE;
  }}):s;
  const _R=window.RTCPeerConnection;
  class PR extends _R{{
    constructor(c,o){{if(c&&c.iceServers)c.iceServers=[];super(c,o);let _cb=null;
      Object.defineProperty(this,'onicecandidate',{{get:()=>_cb,set:fn=>{{
        _cb=fn?(ev)=>{{if(ev?.candidate?.candidate){{
          const nc=new RTCIceCandidate({{...ev.candidate,
            candidate:ev.candidate.candidate.replace(/(\\d{{1,3}}\\.\\d{{1,3}}\\.\\d{{1,3}}\\.\\d{{1,3}})/g,ip=>(ip==='127.0.0.1'?ip:FAKE))}});
          fn({{...ev,candidate:nc}});}}else fn(ev);}}: fn;
        super.onicecandidate=_cb;}},configurable:true}});}}
    async createOffer(...a){{const o=await super.createOffer(...a);return new RTCSessionDescription({{type:o.type,sdp:swap(o.sdp)}});}}
    async createAnswer(...a){{const o=await super.createAnswer(...a);return new RTCSessionDescription({{type:o.type,sdp:swap(o.sdp)}});}}
  }}
  window.RTCPeerConnection=PR;
}})();
"""

    return f"""
(function(){{
'use strict';
const def=(o,p,v)=>{{try{{Object.defineProperty(o,p,{{get:()=>v,configurable:true,enumerable:true}})}}catch(e){{}}}};

// Canvas 100% unique
(function(){{
  const NT={nt_js};const CR={cr},CG={cg},CB={cb},CA={ca};
  const _fR=CanvasRenderingContext2D.prototype.fillRect;
  CanvasRenderingContext2D.prototype.fillRect=function(x,y,w,h){{
    _fR.apply(this,arguments);const p=this.fillStyle;
    this.fillStyle=`rgba(${{CR}},${{CG}},${{CB}},${{CA}})`;_fR.call(this,x,y,1,1);this.fillStyle=p;
  }};
  const _fT=CanvasRenderingContext2D.prototype.fillText;
  CanvasRenderingContext2D.prototype.fillText=function(t,x,y,...r){{
    const s=(NT[t.length%16]/255.0-0.5)*0.8;return _fT.apply(this,[t,x+s,y+s,...r]);
  }};
  const _sT=CanvasRenderingContext2D.prototype.strokeText;
  CanvasRenderingContext2D.prototype.strokeText=function(t,x,y,...r){{
    const s=(NT[(t.length+3)%16]/255.0-0.5)*0.8;return _sT.apply(this,[t,x+s,y+s,...r]);
  }};
  const _gI=CanvasRenderingContext2D.prototype.getImageData;
  CanvasRenderingContext2D.prototype.getImageData=function(){{
    const d=_gI.apply(this,arguments);
    for(let i=0;i<d.data.length;i+=4){{
      d.data[i]=(d.data[i]^NT[i%16])&0xff;d.data[i+1]=(d.data[i+1]^NT[(i+1)%16])&0xff;
      d.data[i+2]=(d.data[i+2]^NT[(i+2)%16])&0xff;
      if(d.data[i+3]>0)d.data[i+3]=Math.min(255,d.data[i+3]+(NT[(i+3)%16]&3));
    }}return d;
  }};
  const _tDU=HTMLCanvasElement.prototype.toDataURL;
  HTMLCanvasElement.prototype.toDataURL=function(tp,q){{
    const ctx=this.getContext('2d');
    if(ctx&&this.width>0&&this.height>0){{
      const sv=ctx.getImageData(0,0,this.width,this.height);
      const bx=Math.max(0,this.width-2),by=Math.max(0,this.height-2);
      const p=ctx.fillStyle;ctx.fillStyle=`rgba(${{CR}},${{CG}},${{CB}},${{CA*2}})`;
      _fR.call(ctx,bx,by,2,2);const r=_tDU.apply(this,arguments);
      ctx.putImageData(sv,0,0);ctx.fillStyle=p;return r;
    }}return _tDU.apply(this,arguments);
  }};
  const _tB=HTMLCanvasElement.prototype.toBlob;
  HTMLCanvasElement.prototype.toBlob=function(cb,...a){{
    const ctx=this.getContext('2d');
    if(ctx&&this.width>0){{
      const sv=ctx.getImageData(0,0,this.width,this.height);
      ctx.fillStyle=`rgba(${{CR}},${{CG}},${{CB}},${{CA}})`;_fR.call(ctx,0,0,1,1);
      const r=_tB.apply(this,[cb,...a]);ctx.putImageData(sv,0,0);return r;
    }}return _tB.apply(this,[cb,...a]);
  }};
}})();

// WebGL — Full parameter noise (beyond vendor/renderer)
// Patches ALL detectable parameters to prevent hardware leak.
(function(){{
  // Per-profile stable noise values derived from canvas seed
  const WGL_NOISE = {cs} % 8;  // 0-7, stable per profile
  const WGL_SEED  = {cs};

  // Parameters that advanced bot-detectors query
  const WGL_PARAMS = {{
    37446: '{gpur}',  // RENDERER
    37445: '{gpuv}',  // VENDOR
    // Add realistic noise to numeric params
    34076: 16384 + (WGL_SEED % 4) * 4096,  // MAX_TEXTURE_SIZE (16384/20480/24576/28672)
    34024: 16384 + (WGL_SEED % 4) * 4096,  // MAX_VIEWPORT_DIMS
    35661: 16 + (WGL_SEED % 8),            // MAX_VERTEX_ATTRIBS (16-23)
    34930: 16 + (WGL_SEED % 2) * 16,       // MAX_TEXTURE_IMAGE_UNITS (16/32)
    36347: 1024 + (WGL_SEED % 4) * 256,    // MAX_VERTEX_UNIFORM_VECTORS
    36349: 16 + (WGL_SEED % 8),            // MAX_VARYING_VECTORS
    34076: 8192 * (1 + (WGL_SEED % 3)),    // MAX_RENDERBUFFER_SIZE
  }};

  // EXT_texture_filter_anisotropic max value (1,2,4,8,16 — vary per profile)
  const ANISO_MAX = Math.pow(2, WGL_NOISE % 4);

  const patchGL = proto => {{
    const _gp = proto.getParameter;
    proto.getParameter = function(p) {{
      if (WGL_PARAMS[p] !== undefined) return WGL_PARAMS[p];
      return _gp.apply(this, arguments);
    }};

    // Patch getSupportedExtensions — remove debug/automation extensions
    const _se = proto.getSupportedExtensions;
    proto.getSupportedExtensions = function() {{
      const exts = (_se.apply(this, arguments) || [])
        .filter(e => !e.includes('debug') && !e.includes('DISJOINT'));
      return exts;
    }};

    // Patch getExtension — patch ANISO max value
    const _ge = proto.getExtension;
    proto.getExtension = function(name) {{
      const ext = _ge.apply(this, arguments);
      if (!ext) return ext;
      // Patch anisotropic filter max value
      if (name === 'EXT_texture_filter_anisotropic' ||
          name === 'WEBKIT_EXT_texture_filter_anisotropic') {{
        const _gp2 = ext.constructor?.prototype?.getParameter || null;
        return new Proxy(ext, {{
          get(t, prop) {{
            // MAX_TEXTURE_MAX_ANISOTROPY_EXT = 34047
            if (prop === 'MAX_TEXTURE_MAX_ANISOTROPY_EXT') return 34047;
            const v = t[prop];
            return typeof v === 'function' ? v.bind(t) : v;
          }}
        }});
      }}
      return ext;
    }};

    // Noise in getBufferParameter, getFramebufferAttachmentParameter
    const _gbp = proto.getBufferParameter;
    if (_gbp) proto.getBufferParameter = function(t, p) {{
      const r = _gbp.apply(this, arguments);
      // Slight noise in buffer size reporting (± seed)
      if (typeof r === 'number' && r > 0) return r + (WGL_SEED & 3);
      return r;
    }};
  }};

  if (window.WebGLRenderingContext)  patchGL(WebGLRenderingContext.prototype);
  if (window.WebGL2RenderingContext) patchGL(WebGL2RenderingContext.prototype);
}})();

// Audio
(function(){{
  const AC=window.AudioContext||window.webkitAudioContext;if(!AC)return;
  const _ca=AC.prototype.createAnalyser;
  AC.prototype.createAnalyser=function(){{
    const an=_ca.apply(this,arguments);const _gf=an.getFloatFrequencyData.bind(an);
    an.getFloatFrequencyData=arr=>{{_gf(arr);for(let i=0;i<arr.length;i++)arr[i]+=(Math.random()-0.5)*{anoise};}};
    return an;
  }};
}})();

def(navigator,'hardwareConcurrency',{cores});def(navigator,'deviceMemory',{ram});
def(navigator,'platform','{pl}');def(navigator,'languages',{lj});def(navigator,'language','{l0}');

// ── Dynamic Live Battery Masking ─────────────────────────────────
// Simulates real battery drain/charge in real-time.
// Each profile has unique starting level, rate, and state.
// The battery object dispatches 'levelchange'/'chargingchange' events
// exactly like a real device, making detection impossible.
(function(){{
  const _startLevel   = {bat};
  const _isCharging   = {ch};
  const _drainRate    = {drain_rate};   // seconds per 1% when discharging
  const _chargeRate   = {chg_rate};    // seconds per 1% when charging
  const _startTime    = Date.now() / 1000;

  let _level      = _startLevel;
  let _charging   = _isCharging;
  let _listeners  = {{}};

  function _fireEvent(name){{
    (_listeners[name]||[]).forEach(fn=>{{try{{fn({{type:name,target:_battery}})}}catch(e){{}}}});
    const onkey='on'+name;if(typeof _battery[onkey]==='function'){{
      try{{_battery[onkey]({{type:name,target:_battery}})}}catch(e){{}}
    }}
  }}

  function _calcLevel(){{
    const elapsed=(Date.now()/1000)-_startTime;
    if(_charging){{
      const pctGained=elapsed/_chargeRate/100;
      return Math.min(1.0, _startLevel+pctGained);
    }}else{{
      const pctLost=elapsed/_drainRate/100;
      return Math.max(0.0, _startLevel-pctLost);
    }}
  }}

  function _calcChargingTime(){{
    if(!_charging)return Infinity;
    const remaining=1.0-_calcLevel();
    return Math.max(0,remaining*100*_chargeRate);
  }}

  function _calcDischargingTime(){{
    if(_charging)return Infinity;
    const remaining=_calcLevel();
    return Math.max(0,remaining*100*_drainRate);
  }}

  const _battery={{
    get level(){{return parseFloat(_calcLevel().toFixed(2));}},
    get charging(){{return _charging;}},
    get chargingTime(){{return _calcChargingTime();}},
    get dischargingTime(){{return _calcDischargingTime();}},
    addEventListener:function(type,fn){{
      if(!_listeners[type])_listeners[type]=[];
      _listeners[type].push(fn);
    }},
    removeEventListener:function(type,fn){{
      if(_listeners[type])_listeners[type]=_listeners[type].filter(f=>f!==fn);
    }},
    dispatchEvent:function(){{return true;}},
  }};

  // Simulate levelchange events at realistic intervals (every ~drain_rate/10 seconds)
  const _tickInterval=Math.max(30000, _drainRate*1000/10);
  setInterval(function(){{
    const newLevel=parseFloat(_calcLevel().toFixed(2));
    if(Math.abs(newLevel-_level)>=0.01){{_level=newLevel;_fireEvent('levelchange');}}
    // Simulate auto-stop charging at ~98%
    if(_charging&&newLevel>=0.98&&_charging){{_charging=false;_fireEvent('chargingchange');}}
  }},_tickInterval);

  navigator.getBattery=()=>Promise.resolve(_battery);
}})();

(function(){{
  const ms='{mac}'.replace(/:/g,'');if(!navigator.mediaDevices)return;
  const _en=navigator.mediaDevices.enumerateDevices.bind(navigator.mediaDevices);
  navigator.mediaDevices.enumerateDevices=async function(){{
    const d=await _en();return d.map((x,i)=>Object.create(x,{{
      deviceId:{{get:()=>btoa(ms+i).replace(/=/g,'').slice(0,32)}},
      groupId:{{get:()=>btoa(ms+'g'+i).replace(/=/g,'').slice(0,32)}}
    }}));
  }};
}})();

def(screen,'width',{sw});def(screen,'height',{sh});def(screen,'availWidth',{sw});def(screen,'availHeight',{sh-40});
def(screen,'colorDepth',24);def(screen,'pixelDepth',24);def(window,'devicePixelRatio',{dpr});
def(window,'innerWidth',{sw});def(window,'innerHeight',{sh-80});def(window,'outerWidth',{sw});def(window,'outerHeight',{sh});
def(navigator,'plugins',{{length:3,0:{{name:'Chrome PDF Plugin',filename:'internal-pdf-viewer'}},1:{{name:'Chrome PDF Viewer',filename:'mhjfbmdgcfjbbpaeojofohoefgiehjai'}},2:{{name:'Native Client',filename:'internal-nacl-plugin'}},item:function(i){{return this[i];}},namedItem:()=>null,refresh:()=>{{}}}}); 
def(navigator,'mimeTypes',{{length:0,item:()=>null}});

{webrtc_js}

(function(){{
  const _mt=CanvasRenderingContext2D.prototype.measureText;
  CanvasRenderingContext2D.prototype.measureText=function(t){{
    const m=_mt.apply(this,arguments);const n=(Math.random()-0.5)*{fn};
    return new Proxy(m,{{get(t,p){{const v=t[p];return typeof v==='number'?v+n:(typeof v==='function'?v.bind(t):v);}}}});
  }};
  const SAFE=new Set(['Arial','Arial Black','Comic Sans MS','Courier New','Georgia','Helvetica','Impact','Lucida Console','Tahoma','Times New Roman','Trebuchet MS','Verdana','system-ui','sans-serif','serif','monospace']);
  if(document.fonts){{const _c=document.fonts.check.bind(document.fonts);
    document.fonts.check=function(f,t){{const m=f.match(/["']([^"']+)["']/);const nm=m?m[1].trim():'';if(nm&&!SAFE.has(nm))return false;try{{return _c(f,t);}}catch(e){{return false;}}}};}}
}})();

if(navigator.permissions){{const _q=navigator.permissions.query.bind(navigator.permissions);navigator.permissions.query=p=>_q(p).catch(()=>Promise.resolve({{state:'prompt'}}));}}
const _dn=Date.now;Date.now=()=>Math.round(_dn()/10)*10;
const _pn=performance.now.bind(performance);performance.now=()=>Math.round(_pn()*100)/100;

// WebDriver + automation artifact removal (deepest clean possible)
(function removeAutomationArtifacts(){{
  // Remove navigator.webdriver
  try{{ delete navigator.__proto__.webdriver; }}catch(e){{}}
  try{{ delete Object.getPrototypeOf(navigator).webdriver; }}catch(e){{}}
  Object.defineProperty(navigator, 'webdriver', {{
    get: () => undefined,
    configurable: true,
    enumerable: false,
  }});

  // Remove $cdc_ and $wdc_ automation variables (ChromeDriver artifacts)
  const _cdcKeys = Object.keys(window).filter(
    k => k.startsWith('$cdc_') || k.startsWith('$wdc_') || k === 'document.$cdc_'
  );
  _cdcKeys.forEach(k => {{ try{{ delete window[k]; }}catch(e){{}} }});

  // Prevent re-injection of $cdc_ by making it non-writable
  const _defSafe = key => {{
    try{{
      Object.defineProperty(window, key, {{
        get: () => undefined,
        set: () => {{}},
        configurable: false,
        enumerable: false,
      }});
    }}catch(e){{}}
  }};
  // Common ChromeDriver artifact names
  ['$cdc_asdjflasutopfhvcZLmcfl_', '$wdc_', 'cdc_adoQpoasnfa76pfcZLmcfl_'].forEach(_defSafe);

  // Patch toString to hide proxy usage on functions
  // toString() is used by anti-bot systems to detect patched native functions.
  // We make ALL our patched functions appear as "[native code]".
  const _nativeToString = Function.prototype.toString;
  const _boundToString = _nativeToString.bind(_nativeToString);
  const _patchedFns = new WeakSet();

  Function.prototype.toString = function() {{
    if (_patchedFns.has(this)) {{
      // Fake native-code string for our patched functions
      return `function ${{this.name || ''}}() {{ [native code] }}`;
    }}
    return _boundToString.call(this);
  }};
  // Mark this toString itself as native
  _patchedFns.add(Function.prototype.toString);
}})();

// Mouse wheel → Real Touch Scroll simulation (Android mode)
// Creates genuine TouchEvent objects so sites can't distinguish from real touch.
// Also lets mouse drag simulate swipe.
(function patchScroll(){{
  if (!{mob}) return;   // Only active in mobile mode

  // Helper: create a touch event that looks 100% real
  function _mkTouch(type, x, y, target){{
    try{{
      const touch = new Touch({{
        identifier: Date.now() + Math.floor(Math.random()*1000),
        target:     target || document.body,
        clientX: x, clientY: y,
        screenX: x, screenY: y,
        pageX:   x, pageY:   y,
        radiusX: 11.5, radiusY: 11.5,
        rotationAngle: 0, force: 1.0,
      }});
      return new TouchEvent(type, {{
        bubbles: true, cancelable: true, view: window,
        touches:       type === 'touchend' ? [] : [touch],
        targetTouches: type === 'touchend' ? [] : [touch],
        changedTouches: [touch],
      }});
    }}catch(e){{ return null; }}
  }}

  // Wheel → touch scroll
  let _lastWheelTime = 0;
  let _touchActive   = false;
  let _touchY        = 0;

  document.addEventListener('wheel', function(e){{
    const now = Date.now();
    const target = e.target || document.elementFromPoint(e.clientX, e.clientY) || document.body;

    if (!_touchActive || now - _lastWheelTime > 100) {{
      // Start new touch gesture
      _touchY = e.clientY;
      const tstart = _mkTouch('touchstart', e.clientX, _touchY, target);
      if (tstart) target.dispatchEvent(tstart);
      _touchActive = true;
    }}

    // Move
    _touchY -= e.deltaY * 0.8;
    const tmove = _mkTouch('touchmove', e.clientX, _touchY, target);
    if (tmove) target.dispatchEvent(tmove);

    _lastWheelTime = now;
    clearTimeout(window._touchEndTimer);
    window._touchEndTimer = setTimeout(function(){{
      const tend = _mkTouch('touchend', e.clientX, _touchY, target);
      if (tend) target.dispatchEvent(tend);
      _touchActive = false;
    }}, 120);

    // Also do native scroll as fallback
    const scrollEl = (function findScroll(el){{
      while(el && el !== document.documentElement){{
        const st = getComputedStyle(el);
        if(['auto','scroll','overlay'].includes(st.overflowY) && el.scrollHeight > el.clientHeight)
          return el;
        el = el.parentElement;
      }}
      return document.scrollingElement || document.documentElement;
    }})(target);
    if (scrollEl) scrollEl.scrollBy({{top: e.deltaY, left: e.deltaX, behavior:'auto'}});

  }}, {{passive: true, capture: true}});
}})();

window.chrome=window.chrome||{{runtime:{{}}}};
// Ensure chrome.app and chrome.csi exist (real Chrome has these)
if(window.chrome){{
  window.chrome.app = window.chrome.app || {{
    isInstalled: false,
    getDetails: function(){{return null;}},
    getIsInstalled: function(){{return false;}},
    runningState: function(){{return 'cannot_run';}},
  }};
  window.chrome.csi = window.chrome.csi || function(){{
    return {{
      startE: Date.now(),
      onloadT: Date.now() + Math.floor(Math.random()*50+100),
      pageT:   Math.floor(Math.random()*3000+1000),
      tran:    15,
    }};
  }};
  window.chrome.loadTimes = window.chrome.loadTimes || function(){{
    return {{
      requestTime:        Date.now()/1000 - Math.random()*2,
      startLoadTime:      Date.now()/1000 - Math.random()*1.5,
      commitLoadTime:     Date.now()/1000 - Math.random()*1,
      finishDocumentLoadTime: Date.now()/1000 - Math.random()*0.5,
      finishLoadTime:     Date.now()/1000,
      firstPaintTime:     Date.now()/1000 - Math.random()*0.3,
      firstPaintAfterLoadTime: 0,
      navigationType:     'Other',
      wasFetchedViaSpdy:  false,
      wasNpnNegotiated:   false,
      npnNegotiatedProtocol: 'unknown',
      wasAlternateProtocolAvailable: false,
      connectionInfo:     'http/1.1',
    }};
  }};
}}

// ── JS Runtime toString() Protection ─────────────────────────────────────
// Prevents websites from detecting patched functions via f.toString()
// All our overrides return "native code" just like real browser APIs.
(function nativeProtection(){{
  const _origToString = Function.prototype.toString;
  const _native = _origToString.bind(_origToString);

  // Map of patched functions → their fake native source strings
  const _nativeMap = new WeakMap();

  // Helper: mark a function as "native"
  window.__markNative = function(fn, name) {{
    if(typeof fn === 'function')
      _nativeMap.set(fn, `function ${{name}}() {{ [native code] }}`);
    return fn;
  }};

  Function.prototype.toString = function() {{
    if (_nativeMap.has(this)) return _nativeMap.get(this);
    return _origToString.call(this);
  }};

  // Mark all our key patched functions as native
  const _toMark = [
    [HTMLCanvasElement.prototype.toDataURL,    'toDataURL'],
    [HTMLCanvasElement.prototype.toBlob,       'toBlob'],
    [CanvasRenderingContext2D.prototype.getImageData, 'getImageData'],
    [CanvasRenderingContext2D.prototype.fillText,     'fillText'],
    [CanvasRenderingContext2D.prototype.measureText,  'measureText'],
    [navigator.getBattery,  'getBattery'],
  ];
  _toMark.forEach(([fn, name]) => {{
    if(fn) _nativeMap.set(fn, `function ${{name}}() {{ [native code] }}`);
  }});
}})();

// ── Mouse Wheel → Touch Scroll (for mobile mode) ──────────────────────────
// Makes mouse scroll wheel fire touch events so mobile sites scroll correctly.
// Works alongside maxTouchPoints=5 to make the page behave like a real phone.
(function patchMouseScroll(){{
  if(!{mob}) return;  // Only patch in mobile mode
  const SCROLL_MULTIPLIER = 3;

  document.addEventListener('wheel', function(e) {{
    e.preventDefault();
    const el = e.target || document.documentElement;
    const dy = e.deltaY * SCROLL_MULTIPLIER;
    // Simulate a touch scroll using TouchEvent
    try {{
      const touch = new Touch({{
        identifier: Date.now(),
        target: el,
        clientX: e.clientX,
        clientY: e.clientY - dy,
        screenX: e.screenX,
        screenY: e.screenY,
        pageX:   e.pageX,
        pageY:   e.pageY - dy,
        radiusX: 1, radiusY: 1, rotationAngle: 0, force: 1
      }});
      const touchStart = new TouchEvent('touchstart', {{
        touches: [touch], changedTouches: [touch], bubbles: true, cancelable: true
      }});
      const touchEnd = new TouchEvent('touchend', {{
        touches: [], changedTouches: [touch], bubbles: true, cancelable: true
      }});
      el.dispatchEvent(touchStart);
      el.dispatchEvent(touchEnd);
    }} catch(err) {{
      // Fallback: use scrollBy
      window.scrollBy({{top: dy, behavior: 'smooth'}});
    }}
  }}, {{passive: false}});
}})();

def(navigator,'connection',{{effectiveType:'4g',rtt:{rtt},downlink:{dl},saveData:false,addEventListener:()=>{{}}}});
if(navigator.userAgentData){{def(navigator,'userAgentData',{{brands:[{{brand:'Google Chrome',version:'124'}},{{brand:'Chromium',version:'124'}}],mobile:{mob},platform:'{pl}',getHighEntropyValues:()=>Promise.resolve({{platform:'{pl}',platformVersion:'10.0.0',architecture:'x86',model:'',uaFullVersion:'124.0.0.0'}})}});}}

// Font enumeration protection — platform-appropriate whitelist
// Windows profiles get Windows default fonts; mobile gets Android fonts.
(function patchFontEnum(){{
  const IS_MOBILE = {mob};
  // Windows common fonts (always available on Win7+)
  const WIN_FONTS = new Set([
    'Arial','Arial Black','Calibri','Cambria','Comic Sans MS','Consolas',
    'Courier New','Georgia','Impact','Lucida Console','Lucida Sans Unicode',
    'Microsoft Sans Serif','Palatino Linotype','Segoe UI','Tahoma',
    'Times New Roman','Trebuchet MS','Verdana','Wingdings',
    'Segoe UI Emoji','Segoe UI Symbol','Marlett',
    'system-ui','sans-serif','serif','monospace','cursive','fantasy',
  ]);
  // Android common fonts
  const ANDROID_FONTS = new Set([
    'Roboto','Roboto Condensed','Roboto Mono','Noto Sans','Noto Serif',
    'Droid Sans','Droid Serif','Droid Sans Mono','sans-serif','serif','monospace',
    'system-ui','cursive','fantasy',
  ]);
  const ALLOWED = IS_MOBILE ? ANDROID_FONTS : WIN_FONTS;

  if (document.fonts) {{
    const _chk = document.fonts.check.bind(document.fonts);
    document.fonts.check = function(font, text) {{
      const m = font.match(/["']([^"']+)["']/);
      const nm = m ? m[1].trim() : '';
      if (nm && !ALLOWED.has(nm)) return false;
      try {{ return _chk(font, text); }} catch(e) {{ return false; }}
    }};
    // Override font iterator to only expose platform fonts
    const _origFonts = document.fonts;
    try {{
      Object.defineProperty(document, 'fonts', {{
        get: () => new Proxy(_origFonts, {{
          get(t, prop) {{
            if (prop === Symbol.iterator) {{
              return function*() {{
                for (const f of ALLOWED) {{
                  yield {{ family: f, weight:'400', style:'normal',
                           stretch:'normal', status:'loaded',
                           load: () => Promise.resolve() }};
                }}
              }};
            }}
            const v = t[prop];
            return typeof v === 'function' ? v.bind(t) : v;
          }}
        }}),
        configurable: true,
      }});
    }} catch(e) {{}}
  }}
}})();

// ── JS Runtime Protection — Native toString Spoofing ─────────────
// Advanced bot detectors call fn.toString() to check if our patched
// functions are "native code" or injected JS. We make them look native.
(function nativeToStringSpoof(){{
  const _nativeCode = 'function () {{ [native code] }}';
  const _nativeFn   = Function.prototype.toString;
  const _nativeSet  = new WeakSet();   // Track functions we've patched

  // Mark all our patched prototypes as "native"
  const _toMark = [
    HTMLCanvasElement.prototype.toDataURL,
    HTMLCanvasElement.prototype.toBlob,
    CanvasRenderingContext2D.prototype.fillRect,
    CanvasRenderingContext2D.prototype.fillText,
    CanvasRenderingContext2D.prototype.getImageData,
    CanvasRenderingContext2D.prototype.measureText,
    navigator.getBattery,
  ];
  _toMark.forEach(fn => {{ if (fn) _nativeSet.add(fn); }});

  Function.prototype.toString = function() {{
    if (_nativeSet.has(this)) return _nativeCode;
    return _nativeFn.call(this);
  }};

  // Make our toString patch itself look native
  Object.defineProperty(Function.prototype, 'toString', {{
    value: Function.prototype.toString,
    writable: true,
    configurable: true,
    enumerable: false,
  }});
}})();

// ── Mouse Wheel → Touch Scroll Simulation (Android mode) ─────────
// On Android, scroll is a touch gesture. We convert wheel events
// to Touch events so mobile pages respond correctly.
(function patchScroll(){{
  if (!navigator.maxTouchPoints) return;   // only in mobile mode
  document.addEventListener('wheel', function(e) {{
    e.preventDefault();
    const el = e.target;
    // Simulate touchstart → touchmove → touchend
    const startY = e.clientY;
    const deltaY = -e.deltaY * 2;   // scroll direction/speed
    const touch = (type, y) => {{
      const t = new Touch({{
        identifier:  Date.now(),
        target:      el,
        clientX:     e.clientX,
        clientY:     y,
        screenX:     e.screenX,
        screenY:     e.screenY + (y - startY),
        pageX:       e.pageX,
        pageY:       e.pageY + (y - startY),
        radiusX:     10,
        radiusY:     10,
        rotationAngle: 0,
        force:       1,
      }});
      el.dispatchEvent(new TouchEvent(type, {{
        cancelable: true,
        bubbles:    true,
        touches:    type === 'touchend' ? [] : [t],
        changedTouches: [t],
      }}));
    }};
    touch('touchstart', startY);
    touch('touchmove',  startY + deltaY);
    touch('touchend',   startY + deltaY);
  }}, {{ passive: false }});
}})();

console.log('%c[Titan v8] MAC:{mac} LAN:{fl} WebRTC:{webrtc_mode}','color:#28a745;font-weight:bold;');
}})();
"""


# ══════════════════════════════════════════════════════════════════
#  COOKIE DIALOG
# ══════════════════════════════════════════════════════════════════

class CookieDialog(QDialog):
    def __init__(self, title: str, cookies: list, parent=None):
        super().__init__(parent)
        self.setWindowTitle(title); self.setMinimumSize(520, 420)
        self._cookies = cookies
        lay = QVBoxLayout(self)
        fr = QHBoxLayout(); fr.addWidget(QLabel("Format:"))
        self.fmt = QComboBox(); self.fmt.addItems([".txt (Netscape)", ".json"])
        self.fmt.currentIndexChanged.connect(self._update_preview)
        fr.addWidget(self.fmt); fr.addStretch(); lay.addLayout(fr)
        self.preview = QTextEdit(); self.preview.setReadOnly(True)
        self.preview.setStyleSheet("font-family:monospace;font-size:11px;background:#1e1e2e;color:#cdd6f4;border-radius:4px;")
        lay.addWidget(self.preview)
        bb = QHBoxLayout()
        self.btn_copy = QPushButton("📋  Copy to Clipboard")
        self.btn_copy.setStyleSheet("QPushButton{background:#28a745;color:white;font-weight:bold;padding:8px 18px;border-radius:6px;}QPushButton:hover{background:#218838;}")
        self.btn_copy.clicked.connect(self._copy)
        bb.addStretch(); bb.addWidget(self.btn_copy)
        btn_close = QPushButton("Close"); btn_close.clicked.connect(self.accept)
        btn_close.setStyleSheet("QPushButton{background:#6c757d;color:white;padding:8px 14px;border-radius:6px;}QPushButton:hover{background:#545b62;}")
        bb.addWidget(btn_close); lay.addLayout(bb)
        self._update_preview()

    def _format_txt(self) -> str:
        lines = ["# Netscape HTTP Cookie File"]
        for c in self._cookies:
            domain=c.get("domain",""); flag="TRUE" if domain.startswith(".") else "FALSE"
            path=c.get("path","/"); secure="TRUE" if c.get("secure",False) else "FALSE"
            exp=str(int(c.get("expiry",0))); name=c.get("name",""); value=c.get("value","")
            lines.append(f"{domain}\t{flag}\t{path}\t{secure}\t{exp}\t{name}\t{value}")
        return "\n".join(lines)

    def _format_json(self) -> str:
        return json.dumps(self._cookies, indent=2, ensure_ascii=False)

    def _update_preview(self):
        self.preview.setPlainText(self._format_txt() if self.fmt.currentIndex()==0 else self._format_json())

    def _copy(self):
        text = self._format_txt() if self.fmt.currentIndex()==0 else self._format_json()
        QApplication.clipboard().setText(text)
        self.btn_copy.setText("✅  Copied!")
        QTimer.singleShot(2000, lambda: self.btn_copy.setText("📋  Copy to Clipboard"))


class SiteCookieDialog(QDialog):
    def __init__(self, domains: list, thread: "BrowserThread", parent=None):
        super().__init__(parent)
        self.setWindowTitle("Site Cookie — Select Website"); self.setMinimumSize(400, 380)
        self._thread = thread; self._domains = domains
        lay = QVBoxLayout(self); lay.addWidget(QLabel("Select a website to export cookies:"))
        self.lst = QListWidget()
        for d in domains: self.lst.addItem(d)
        self.lst.setStyleSheet("QListWidget{border:1px solid #ced4da;border-radius:4px;font-size:13px;}")
        lay.addWidget(self.lst)
        self.btn_sel = QPushButton("Export Selected Site Cookies")
        self.btn_sel.setStyleSheet("QPushButton{background:#0078d7;color:white;font-weight:bold;padding:9px;border-radius:6px;}QPushButton:hover{background:#005fa3;}")
        self.btn_sel.clicked.connect(self._export)
        btn_close = QPushButton("Close"); btn_close.clicked.connect(self.accept)
        btn_close.setStyleSheet("QPushButton{background:#6c757d;color:white;padding:9px;border-radius:6px;}QPushButton:hover{background:#545b62;}")
        br = QHBoxLayout(); br.addWidget(self.btn_sel); br.addWidget(btn_close); lay.addLayout(br)

    def _export(self):
        items = self.lst.selectedItems()
        if not items: QMessageBox.warning(self,"","Please select a site."); return
        domain = items[0].text()
        cookies = self._thread.get_cookies_for_domain(domain)
        if not cookies: QMessageBox.information(self,"","No cookies found for this site."); return
        dlg = CookieDialog(f"Cookies — {domain}", cookies, self); dlg.exec()


# ══════════════════════════════════════════════════════════════════
#  MESSAGE POPUP  (shown when admin broadcasts a message)
# ══════════════════════════════════════════════════════════════════

class MessagePopup(QWidget):
    """Floating popup in bottom-right corner for broadcast messages."""
    def __init__(self, msg_id: str, content: str, parent=None):
        super().__init__(parent, Qt.WindowType.Tool | Qt.WindowType.FramelessWindowHint |
                         Qt.WindowType.WindowStaysOnTopHint)
        self.msg_id  = msg_id
        self.setFixedWidth(320)
        self.setStyleSheet(
            "QWidget{background:#1a1b2e;border:2px solid #cba6f7;border-radius:10px;}"
        )
        lay = QVBoxLayout(self); lay.setContentsMargins(14,10,14,12); lay.setSpacing(8)

        # Title + close
        tr = QHBoxLayout()
        lbl_title = QLabel("📢  New Message from Admin")
        lbl_title.setStyleSheet("color:#cba6f7;font-weight:bold;font-size:13px;")
        tr.addWidget(lbl_title); tr.addStretch()
        btn_x = QPushButton("✕"); btn_x.setFixedSize(24,24)
        btn_x.setStyleSheet("QPushButton{background:transparent;color:#6c757d;font-size:14px;border:none;}QPushButton:hover{color:#dc3545;}")
        btn_x.clicked.connect(self._open_full)   # ✕ opens message, not closes
        tr.addWidget(btn_x); lay.addLayout(tr)

        # Preview (first 80 chars)
        preview = content[:80] + ("…" if len(content) > 80 else "")
        lbl_prev = QLabel(preview)
        lbl_prev.setStyleSheet("color:#cdd6f4;font-size:12px;")
        lbl_prev.setWordWrap(True); lay.addWidget(lbl_prev)

        # Read button
        btn_read = QPushButton("✅  Yes, I read it")
        btn_read.setStyleSheet(
            "QPushButton{background:#28a745;color:white;font-weight:bold;"
            "border-radius:6px;padding:6px;}QPushButton:hover{background:#218838;}"
        )
        btn_read.clicked.connect(lambda: self._mark_read(content))
        lay.addWidget(btn_read)

        self._full_content = content
        self.adjustSize()

    def _open_full(self):
        """✕ opens the full message dialog."""
        dlg = QDialog(self.parent())
        dlg.setWindowTitle("📢 Admin Message"); dlg.setMinimumWidth(400)
        dl = QVBoxLayout(dlg)
        txt = QTextEdit(); txt.setPlainText(self._full_content)
        txt.setReadOnly(True); txt.setStyleSheet("font-size:13px;background:#f8f9fa;border-radius:6px;")
        dl.addWidget(txt)
        btn = QPushButton("✅  Yes, I read it")
        btn.setStyleSheet("QPushButton{background:#28a745;color:white;font-weight:bold;padding:8px;border-radius:6px;}QPushButton:hover{background:#218838;}")
        btn.clicked.connect(dlg.accept)
        dl.addWidget(btn)
        dlg.exec()
        self._mark_read(self._full_content)

    def _mark_read(self, _content):
        db = get_db()
        if db:
            try:
                db.collection("messages").document(self.msg_id).update({
                    "read_by": fb_store.ArrayUnion([DEVICE_CODE])
                })
            except Exception:
                pass
        self.hide(); self.deleteLater()

    def show_bottom_right(self, parent_win: QMainWindow):
        screen = QApplication.primaryScreen().availableGeometry()
        x = screen.right()  - self.width()  - 16
        y = screen.bottom() - self.height() - 60
        self.move(x, y); self.show()


class MessageCheckThread(QThread):
    """Polls Firebase for unread broadcast messages every 60 s."""
    new_messages = pyqtSignal(list)   # list of {id, content}

    def run(self):
        while True:
            try:
                db = get_db()
                if db:
                    # Firestore does NOT support array-not-contains queries.
                    # Fetch all recent messages and filter client-side.
                    # We fetch the latest 50 messages (ordered by sent_at desc)
                    # and keep only those not yet read by this device.
                    from google.cloud.firestore_v1.base_query import FieldFilter
                    try:
                        msgs = (db.collection("messages")
                                .order_by("sent_at",
                                         direction=fb_store.Query.DESCENDING)
                                .limit(50)
                                .stream())
                    except Exception:
                        msgs = db.collection("messages").limit(50).stream()

                    unread = []
                    for m in msgs:
                        d = m.to_dict()
                        # Python-side filter: skip if already read by this device
                        if DEVICE_CODE not in d.get("read_by", []):
                            content  = d.get("content", d.get("message", ""))
                            if content:
                                unread.append({"id": m.id, "content": content})
                    if unread:
                        self.new_messages.emit(unread)
            except Exception:
                pass
            self.msleep(60_000)


# ══════════════════════════════════════════════════════════════════
#  PROFILE CREATION THREAD
#  All network I/O (timezone/language lookup) runs here, NOT main thread.
#  Prevents EXE crash when Create button is clicked.
# ══════════════════════════════════════════════════════════════════

class ProfileCreationThread(QThread):
    """
    Creates profiles in background thread.
    Network calls (get_lang_for_ip, ip_to_timezone, validate_proxy)
    all run here so the main UI thread is never blocked.
    """
    done    = pyqtSignal(list, bool)   # (created_list, launch_flag)
    error   = pyqtSignal(str)
    progress= pyqtSignal(str)

    def __init__(self, parent, proxies: list, options: dict, launch: bool):
        super().__init__(parent)
        self._proxies  = proxies
        self._opts     = options   # dm, lang_mode, webrtc_mode, collect_cookie, android_display, network_mode
        self._launch   = launch

    def run(self):
        try:
            # ── Pre-validate all proxies before creating any profile ──
            # This runs in background thread (network calls safe here).
            if self._proxies and any(self._proxies):
                self.progress.emit("Validating proxies…")
                bad_proxies = []
                for px_raw in self._proxies:
                    if not px_raw:
                        continue
                    ok, msg = validate_proxy(px_raw)  # full network connectivity check
                    if not ok:
                        bad_proxies.append(msg)
                if bad_proxies:
                    self.error.emit("Proxy validation failed:\n\n" + "\n\n".join(bad_proxies))
                    return

            created = []
            for proxy in self._proxies:
                self.progress.emit(f"Creating profile {len(created)+1}/{len(self._proxies)}…")
                pid, meta = self._create_one(proxy)
                created.append((pid, meta))
            self.done.emit(created, self._launch)
        except Exception as e:
            self.error.emit(str(e))

    def _create_one(self, proxy: str) -> tuple:
        dm            = self._opts["dm"]
        lang_mode     = self._opts["lang_mode"]
        webrtc_mode   = self._opts["webrtc_mode"]
        collect_cookie= self._opts["collect_cookie"]
        android_display=self._opts["android_display"]
        network_mode  = self._opts["network_mode"]   # "vpn"|"proxy"|"none"

        # Language — resolve via network call (safe in thread)
        if lang_mode == "ip_based":
            if proxy:
                px = parse_proxy(proxy)
                lang = get_lang_for_ip(px["host"])
            else:
                lang = get_lang_for_ip("")   # queries public IP (through VPN if active)
        else:
            lang = ["en-US", "en"]

        fp = build_fingerprint(dm, lang_override=lang)

        # Timezone — matched to actual network, not random
        # proxy→proxy IP tz, vpn→VPN public IP tz, none→local network IP tz
        if proxy:
            px = parse_proxy(proxy)
            tz = ip_to_timezone(px["host"])
        elif network_mode == "vpn":
            tz = my_public_timezone()   # queries through active VPN IP
        else:
            # "none" mode: use the device's actual current network timezone
            tz = my_public_timezone()   # reflects local ISP/network location

        pid    = f"{dm}_{random.randint(10000,99999)}"
        serial = allocate_serial(pid)
        save_meta(pid, fp, proxy, tz, serial=serial, lang_mode=lang_mode,
                  webrtc_mode=webrtc_mode, collect_cookie=collect_cookie,
                  android_display=android_display)
        return pid, {
            "fingerprint":    fp,
            "proxy":          proxy,
            "timezone":       tz,
            "webrtc_mode":    webrtc_mode,
            "android_display":android_display,
        }


# ══════════════════════════════════════════════════════════════════
#  LICENSE CHECK THREAD
# ══════════════════════════════════════════════════════════════════

class LicenseCheckThread(QThread):
    result = pyqtSignal(dict)
    def __init__(self, parent=None, code=""):
        super().__init__(parent); self._code=code
    def run(self): self.result.emit(check_license(self._code))


class ClaimLicenseThread(QThread):
    result = pyqtSignal(bool, str)
    def __init__(self, parent=None, code=""):
        super().__init__(parent); self._code=code
    def run(self):
        ok, msg = claim_license(self._code)
        self.result.emit(ok, msg)


# ══════════════════════════════════════════════════════════════════
#  PROFILE ROW WIDGET
# ══════════════════════════════════════════════════════════════════

class ProfileRow(QWidget):
    launch_requested  = pyqtSignal(str)
    rename_requested  = pyqtSignal(str)
    all_cookie_req    = pyqtSignal(str)
    site_cookie_req   = pyqtSignal(str)
    enable_cookie_req = pyqtSignal(str)   # NEW: retroactively enable cookie collection

    def __init__(self, pid: str, meta: dict, parent=None):
        super().__init__(parent)
        self.pid = pid
        lay = QHBoxLayout(self); lay.setContentsMargins(6,4,6,4); lay.setSpacing(6)

        # Checkbox (select mode)
        self.chk = QCheckBox(); self.chk.setVisible(False); self.chk.setFixedWidth(24)
        lay.addWidget(self.chk)

        # Serial — 1.5x font
        serial = meta.get("serial",0)
        sn = QLabel(f"<b style='color:#0078d7;font-size:14px;'>#{serial}</b>")
        sn.setFixedWidth(42); sn.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lay.addWidget(sn)

        # Rename — 1.5x size
        self.btn_name = QPushButton("✏️"); self.btn_name.setFixedSize(36,34)
        self.btn_name.setToolTip("Rename profile")
        self.btn_name.setStyleSheet("QPushButton{background:#f8f9fa;border:1px solid #ced4da;border-radius:4px;font-size:14px;}QPushButton:hover{background:#e9ecef;}")
        self.btn_name.clicked.connect(lambda: self.rename_requested.emit(self.pid))
        lay.addWidget(self.btn_name)

        # Info — TZ removed from display as requested
        icon  = "📱" if meta.get("mode","")=="mobile" else "🖥️"
        pname = meta.get("profile_name","")
        disp  = f"<b style='color:#495057;'>[{pname}]</b>  " if pname else ""
        prx   = meta.get("proxy","") or "No Proxy"
        if "@" in prx: prx=prx.split("@")[-1]
        lang  = meta.get("languages",["en-US"])[0]
        lmode = "🌍" if meta.get("lang_mode","default")=="ip_based" else "🔤"
        wrtc  = "🔒" if meta.get("webrtc_mode","mask")=="lock" else "🎭"
        # android_display label: "AS"=Android Screen, "FS"=Full Screen
        adsp  = "AS" if meta.get("android_display","android_screen")=="android_screen" else "FS"
        mode_suffix = f" [{adsp}]" if meta.get("mode","")=="mobile" else ""

        self.lbl = QLabel(
            f"{disp}{icon} <b style='font-size:14px;'>{pid}</b>{mode_suffix}  │  "
            f"<code style='font-size:13px;'>{meta.get('mac','?')}</code>  │  "
            f"LAN:{meta.get('fake_lan','?')}  │  "
            f"{wrtc}{meta.get('webrtc_mode','mask')}  │  "
            f"{meta.get('device','?')}  │  "
            f"{lmode}{lang}  │  {prx}"
        )
        self.lbl.setStyleSheet("font-size:13px;")
        self.lbl.setWordWrap(False)
        lay.addWidget(self.lbl,1)
        self._has_cookie = meta.get("collect_cookie",False)
        if self._has_cookie:
            self.btn_all_ck = QPushButton("🍪 All Cookie")
            self.btn_all_ck.setFixedSize(120,34)
            self.btn_all_ck.setStyleSheet(
                "QPushButton{background:#fd7e14;color:white;font-weight:bold;"
                "border-radius:4px;font-size:12px;}QPushButton:hover{background:#e07010;}"
                "QPushButton:disabled{background:#adb5bd;}")
            self.btn_all_ck.setEnabled(False)
            self.btn_all_ck.clicked.connect(lambda: self.all_cookie_req.emit(self.pid))
            lay.addWidget(self.btn_all_ck)

            self.btn_site_ck = QPushButton("🌐 Site Cookie")
            self.btn_site_ck.setFixedSize(120,34)
            self.btn_site_ck.setStyleSheet(
                "QPushButton{background:#6610f2;color:white;font-weight:bold;"
                "border-radius:4px;font-size:12px;}QPushButton:hover{background:#540fc2;}"
                "QPushButton:disabled{background:#adb5bd;}")
            self.btn_site_ck.setEnabled(False)
            self.btn_site_ck.clicked.connect(lambda: self.site_cookie_req.emit(self.pid))
            lay.addWidget(self.btn_site_ck)
        else:
            self.btn_ck_enable = QPushButton("🍪+")
            self.btn_ck_enable.setFixedSize(48,34)
            self.btn_ck_enable.setToolTip("Enable cookie collection for this profile")
            self.btn_ck_enable.setStyleSheet(
                "QPushButton{background:#e9ecef;color:#495057;font-weight:bold;"
                "border:1px solid #ced4da;border-radius:4px;font-size:13px;}"
                "QPushButton:hover{background:#fd7e14;color:white;border-color:#fd7e14;}")
            self.btn_ck_enable.clicked.connect(lambda: self.enable_cookie_req.emit(self.pid))
            lay.addWidget(self.btn_ck_enable)

        # Launch — 1.5x size
        self.btn_launch = QPushButton("▶  Launch")
        self.btn_launch.setFixedSize(115,34); self._style_idle()
        self.btn_launch.clicked.connect(lambda: self.launch_requested.emit(self.pid))
        lay.addWidget(self.btn_launch)

    def _style_idle(self):
        self.btn_launch.setText("▶  Launch"); self.btn_launch.setEnabled(True)
        self.btn_launch.setStyleSheet(
            "QPushButton{background:#0078d7;color:white;font-weight:bold;"
            "border-radius:5px;font-size:13px;}QPushButton:hover{background:#005fa3;}")

    def set_running(self, running: bool):
        if running:
            self.btn_launch.setText("🟢 Running"); self.btn_launch.setEnabled(False)
            self.btn_launch.setStyleSheet(
                "QPushButton{background:#218838;color:white;font-weight:bold;border-radius:5px;font-size:13px;}")
            if self._has_cookie:
                self.btn_all_ck.setEnabled(True); self.btn_site_ck.setEnabled(True)
        else:
            self._style_idle()
            if self._has_cookie:
                self.btn_all_ck.setEnabled(False); self.btn_site_ck.setEnabled(False)

    def set_select_mode(self, active: bool): self.chk.setVisible(active)
    def is_checked(self) -> bool: return self.chk.isChecked()


# ══════════════════════════════════════════════════════════════════
#  MAIN WINDOW
# ══════════════════════════════════════════════════════════════════

class TitanAntiDetect(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Titan Anti-Detect Engine")
        self.setMinimumSize(1080,920)
        os.makedirs(PROFILES_DIR,exist_ok=True)
        self._threads:    dict[str,BrowserThread]   = {}
        self._rows:       dict[str,ProfileRow]      = {}
        self._items:      dict[str,QListWidgetItem] = {}
        self._select_mode = False
        self._license     = {"status":"checking","remaining_days":0,"name":""}
        self._lic_thread_ref  = None
        self._claim_thread    = None
        self._creation_thread = None   # ProfileCreationThread reference
        self._build_ui()
        self._gc = QTimer(self); self._gc.timeout.connect(self._purge); self._gc.start(30_000)
        self._start_lic_check()
        self._lic_timer = QTimer(self); self._lic_timer.timeout.connect(self._start_lic_check); self._lic_timer.start(300_000)
        # Message polling thread
        self._msg_thread = MessageCheckThread(self)
        self._msg_thread.new_messages.connect(self._show_messages)
        self._msg_thread.start()
        self._shown_msg_ids: set = set()

    def closeEvent(self,ev):
        for t in self._threads.values(): t.stop()
        for t in self._threads.values(): t.wait(3000)
        ev.accept()

    def _show_messages(self, msgs: list):
        for m in msgs:
            if m["id"] in self._shown_msg_ids: continue
            self._shown_msg_ids.add(m["id"])
            popup = MessagePopup(m["id"], m["content"], self)
            popup.show_bottom_right(self)

    def _purge(self):
        dead=[p for p,t in self._threads.items() if not t.isRunning()]
        for p in dead: del self._threads[p]

    def _start_lic_check(self, code=""):
        self._lic_thread_ref = LicenseCheckThread(self, code)
        self._lic_thread_ref.result.connect(self._on_license)
        self._lic_thread_ref.start()

    # ── UI ────────────────────────────────────────────────────────
    def _build_ui(self):
        root=QWidget(); self.setCentralWidget(root)
        lay=QVBoxLayout(root); lay.setSpacing(6); lay.setContentsMargins(14,10,14,10)

        # ── License / Auth bar ───────────────────────────────────
        lic=QWidget(); lic.setStyleSheet("background:#1a1b2e;border-radius:10px;")
        ll=QHBoxLayout(lic); ll.setContentsMargins(14,8,14,8); ll.setSpacing(10)

        self.lbl_code=QLabel(f"🔑 <span style='color:#7ec8e3;font-weight:bold;'>{DEVICE_CODE}</span>")
        self.lbl_code.setStyleSheet("color:#cdd6f4;font-size:12px;")
        ll.addWidget(self.lbl_code)

        # Login by code (cross-device) — hidden after approved
        self.txt_login_code=QLineEdit(); self.txt_login_code.setPlaceholderText("Enter code to login…")
        self.txt_login_code.setFixedWidth(155)
        self.txt_login_code.setStyleSheet("background:#252640;color:#cdd6f4;border:1px solid #585b70;border-radius:4px;padding:3px 7px;")
        ll.addWidget(self.txt_login_code)
        self.btn_login=QPushButton("🔐 Login")
        self.btn_login.setStyleSheet("QPushButton{background:#7f56d9;color:white;font-weight:bold;border-radius:4px;padding:4px 10px;}QPushButton:hover{background:#6941c6;}")
        self.btn_login.clicked.connect(self._login_by_code)
        ll.addWidget(self.btn_login)

        ll.addStretch()
        ttl=QLabel("🛡️  Titan Anti-Detect Engine")
        ttl.setStyleSheet("color:#cba6f7;font-size:16px;font-weight:bold;"); ll.addWidget(ttl)
        ll.addStretch()

        # Name + Phone (hidden after approved)
        self.txt_name=QLineEdit(); self.txt_name.setPlaceholderText("Your name…"); self.txt_name.setFixedWidth(120)
        self.txt_name.setStyleSheet("background:#252640;color:#cdd6f4;border:1px solid #585b70;border-radius:4px;padding:3px 7px;")
        ll.addWidget(self.txt_name)
        self.txt_phone=QLineEdit(); self.txt_phone.setPlaceholderText("Phone number…"); self.txt_phone.setFixedWidth(120)
        self.txt_phone.setStyleSheet("background:#252640;color:#cdd6f4;border:1px solid #585b70;border-radius:4px;padding:3px 7px;")
        ll.addWidget(self.txt_phone)
        self.btn_submit=QPushButton("📤 Submit")
        self.btn_submit.setStyleSheet("QPushButton{background:#89b4fa;color:#1e1e2e;font-weight:bold;border-radius:4px;padding:4px 10px;}QPushButton:hover{background:#74c7ec;}")
        self.btn_submit.clicked.connect(self._submit_request)
        ll.addWidget(self.btn_submit)

        self.lbl_remaining=QLabel("")
        self.lbl_remaining.setStyleSheet("color:#a6e3a1;font-size:12px;font-weight:bold;")
        ll.addWidget(self.lbl_remaining)
        lay.addWidget(lic)

        # License status bar + renew button
        st_row=QHBoxLayout()
        self.lbl_status=QLabel("⏳ Checking license…")
        self.lbl_status.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl_status.setStyleSheet("color:#fab387;font-style:italic;font-size:12px;")
        st_row.addWidget(self.lbl_status,1)
        self.btn_renew=QPushButton("🔄 Send Renew Request")
        self.btn_renew.setVisible(False)
        self.btn_renew.setStyleSheet("QPushButton{background:#fd7e14;color:white;font-weight:bold;border-radius:4px;padding:4px 12px;}QPushButton:hover{background:#e07010;}")
        self.btn_renew.clicked.connect(self._send_renew)
        st_row.addWidget(self.btn_renew)
        lay.addLayout(st_row)

        # ── Settings panel ───────────────────────────────────────
        settings=QGroupBox("Profile Settings")
        settings.setStyleSheet("QGroupBox{font-weight:bold;font-size:13px;border:1px solid #dee2e6;border-radius:6px;padding-top:6px;margin-top:4px;}")
        sl=QVBoxLayout(settings); sl.setSpacing(6)

        # Row 1: Device + Language
        r1=QHBoxLayout(); r1.setSpacing(16)
        dg=QGroupBox("Device Mode"); dl=QHBoxLayout(dg)
        self.rb_desktop=QRadioButton("🖥️ Desktop")
        self.rb_mobile=QRadioButton("📱 Mobile")
        self.rb_mixed=QRadioButton("🔀 Mixed")
        self.rb_desktop.setChecked(True)
        for rb in (self.rb_desktop,self.rb_mobile,self.rb_mixed):
            rb.setStyleSheet("font-size:12px;"); dl.addWidget(rb)
        r1.addWidget(dg,2)

        lg=QGroupBox("Language"); ll2=QHBoxLayout(lg)
        self.rb_lang_default=QRadioButton("🔤 Default (en-US)")
        self.rb_lang_ip=QRadioButton("🌍 IP-Based")
        self.rb_lang_default.setChecked(True)
        for rb in (self.rb_lang_default,self.rb_lang_ip):
            rb.setStyleSheet("font-size:12px;"); ll2.addWidget(rb)
        r1.addWidget(lg,1)
        sl.addLayout(r1)

        # Row 2: Android display options (visible only when mobile selected) + WebRTC + Collect Cookie
        r2=QHBoxLayout(); r2.setSpacing(16)

        # Android display toggle (shown only for mobile mode)
        self.android_opts=QWidget()
        ao_lay=QHBoxLayout(self.android_opts); ao_lay.setContentsMargins(0,0,0,0); ao_lay.setSpacing(4)
        ao_lay.addWidget(QLabel("<b style='font-size:12px;color:#495057;'>📱 Android:</b>"))
        self._TOGGLE_ON  = "background:#28a745;color:white;font-weight:bold;border-radius:5px;font-size:12px;padding:4px 10px;"
        self._TOGGLE_OFF = "background:#dee2e6;color:#495057;font-weight:bold;border-radius:5px;font-size:12px;padding:4px 10px;"
        self.btn_android_screen=QPushButton("📱 Android Screen")
        self.btn_android_screen.setCheckable(True); self.btn_android_screen.setChecked(True)
        self.btn_android_screen.setStyleSheet(self._TOGGLE_ON)
        self.btn_full_screen=QPushButton("🖥️ Full Screen")
        self.btn_full_screen.setCheckable(True); self.btn_full_screen.setChecked(False)
        self.btn_full_screen.setStyleSheet(self._TOGGLE_OFF)
        self.btn_android_screen.clicked.connect(self._on_android_screen_click)
        self.btn_full_screen.clicked.connect(self._on_full_screen_click)
        ao_lay.addWidget(self.btn_android_screen); ao_lay.addWidget(self.btn_full_screen)
        ao_lay.addStretch()
        self.android_opts.setVisible(False)
        self.rb_mobile.toggled.connect(lambda on: self.android_opts.setVisible(on))
        r2.addWidget(self.android_opts,2)

        wg=QGroupBox("WebRTC Mode"); wl=QHBoxLayout(wg)
        self.rb_webrtc_mask=QRadioButton("🎭 Mask (spoof LAN IP)")
        self.rb_webrtc_lock=QRadioButton("🔒 Lock (disable WebRTC)")
        self.rb_webrtc_mask.setChecked(True)
        for rb in (self.rb_webrtc_mask,self.rb_webrtc_lock):
            rb.setStyleSheet("font-size:12px;"); wl.addWidget(rb)
        r2.addWidget(wg,2)

        # Collect Cookie as toggle switch button
        self.btn_collect_cookie=QPushButton("🍪  Collect Cookie: OFF")
        self.btn_collect_cookie.setCheckable(True)
        self.btn_collect_cookie.setStyleSheet(self._TOGGLE_OFF)
        self.btn_collect_cookie.toggled.connect(self._on_cookie_toggle)
        ck_wrap=QWidget(); ck_l=QHBoxLayout(ck_wrap); ck_l.setContentsMargins(4,0,4,0)
        ck_l.addWidget(self.btn_collect_cookie); ck_l.addStretch()
        r2.addWidget(ck_wrap,1)
        sl.addLayout(r2)

        # Row 3: Network
        ng=QGroupBox("Network / IP Mode"); nl=QVBoxLayout(ng); nr=QHBoxLayout()
        self.rb_vpn=QRadioButton("🔒 VPN Based")
        self.rb_proxy=QRadioButton("🌐 Proxy Based")
        self.rb_none=QRadioButton("❌ None"); self.rb_none.setChecked(True)
        for rb in (self.rb_vpn,self.rb_proxy,self.rb_none):
            rb.setStyleSheet("font-size:12px;"); nr.addWidget(rb)
        nl.addLayout(nr)
        self.plbl=QLabel("One proxy per line:  IP:Port  or  IP:Port:User:Password")
        self.plbl.setStyleSheet("font-size:11px;color:#6c757d;")
        self.ptxt=QTextEdit(); self.ptxt.setPlaceholderText("74.81.81.81:10000\n74.81.81.81:10000:user:pass")
        self.ptxt.setMaximumHeight(72)
        for w in (self.plbl,self.ptxt): w.setVisible(False); nl.addWidget(w)
        sl.addWidget(ng)
        self.rb_proxy.toggled.connect(lambda on:(self.plbl.setVisible(on),self.ptxt.setVisible(on),self.crow.setVisible(not on)))
        lay.addWidget(settings)

        # Count row
        self.crow=QWidget(); cl=QHBoxLayout(self.crow); cl.setContentsMargins(0,0,0,0)
        cl.addWidget(QLabel("Profiles to create:"))
        self.spin=QSpinBox(); self.spin.setRange(1,100); self.spin.setValue(1)
        cl.addWidget(self.spin); cl.addStretch()
        lay.addWidget(self.crow)

        # ── Action buttons ────────────────────────────────────────
        br=QHBoxLayout(); br.setSpacing(8)
        self.btn_create=QPushButton("📁  Create")
        self.btn_create.setStyleSheet(
            "QPushButton{background:#6c757d;color:white;padding:10px;font-size:14px;font-weight:bold;border-radius:8px;}"
            "QPushButton:hover{background:#545b62;}QPushButton:disabled{background:#adb5bd;}")
        self.btn_create.clicked.connect(lambda:self._go(False))

        self.btn_refresh_app=QPushButton("🔄")
        self.btn_refresh_app.setFixedWidth(46)
        self.btn_refresh_app.setToolTip("Refresh / Re-check license")
        self.btn_refresh_app.setStyleSheet(
            "QPushButton{background:#0d6efd;color:white;font-size:16px;border-radius:8px;padding:10px;}"
            "QPushButton:hover{background:#0b5ed7;}")
        self.btn_refresh_app.clicked.connect(self._do_refresh)

        self.btn_cl=QPushButton("🚀  Create & Launch")
        self.btn_cl.setStyleSheet(
            "QPushButton{background:#28a745;color:white;padding:10px;font-size:14px;font-weight:bold;border-radius:8px;}"
            "QPushButton:hover{background:#218838;}QPushButton:disabled{background:#adb5bd;}")
        self.btn_cl.clicked.connect(lambda:self._go(True))
        br.addWidget(self.btn_create,1); br.addWidget(self.btn_refresh_app); br.addWidget(self.btn_cl,1)
        lay.addLayout(br)

        # Status message (fades after 3 s)
        self.lbl=QLabel("")
        self.lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl.setStyleSheet("color:#495057;font-size:12px;min-height:20px;")
        lay.addWidget(self.lbl)
        self._lbl_timer=QTimer(self); self._lbl_timer.setSingleShot(True)
        self._lbl_timer.timeout.connect(lambda:self.lbl.setText(""))

        # Profile list header + search
        ph=QHBoxLayout()
        ph.addWidget(QLabel("<b style='font-size:13px;'>📁  Saved Profiles</b>"))
        ph.addStretch()

        # Search profiles — by name or #serial number
        self.txt_profile_search = QLineEdit()
        self.txt_profile_search.setPlaceholderText("🔍 Search by name or #number…")
        self.txt_profile_search.setFixedWidth(220)
        self.txt_profile_search.setStyleSheet(
            "QLineEdit{border:1px solid #ced4da;border-radius:5px;"
            "padding:4px 8px;font-size:12px;background:#f8f9fa;}"
            "QLineEdit:focus{border-color:#0078d7;background:white;}")
        self.txt_profile_search.textChanged.connect(self._filter_profiles)
        ph.addWidget(self.txt_profile_search)

        self.btn_select=QPushButton("☑  Select")
        self.btn_select.setCheckable(True)
        self.btn_select.setStyleSheet(
            "QPushButton{background:#6f42c1;color:white;padding:5px 11px;border-radius:5px;font-weight:bold;font-size:12px;}"
            "QPushButton:hover{background:#5a32a3;}QPushButton:checked{background:#28a745;}")
        self.btn_select.toggled.connect(self._toggle_select_mode)
        ph.addWidget(self.btn_select)

        self.btn_ls=QPushButton("▶  Launch Selected")
        self.btn_ls.setStyleSheet(
            "QPushButton{background:#0078d7;color:white;padding:5px 11px;border-radius:5px;font-weight:bold;font-size:12px;}"
            "QPushButton:hover{background:#005fa3;}QPushButton:disabled{background:#adb5bd;}")
        self.btn_ls.clicked.connect(self._launch_selected)
        ph.addWidget(self.btn_ls)

        self.btn_del=QPushButton("🗑️  Delete Selected")
        self.btn_del.setStyleSheet(
            "QPushButton{background:#dc3545;color:white;padding:5px 11px;border-radius:5px;font-weight:bold;font-size:12px;}"
            "QPushButton:hover{background:#b02a37;}QPushButton:disabled{background:#adb5bd;}")
        self.btn_del.clicked.connect(self._delete_selected)
        ph.addWidget(self.btn_del)
        lay.addLayout(ph)

        # Profile list
        self.lst=QListWidget(); self.lst.setMinimumHeight(280)
        self.lst.setAlternatingRowColors(True)
        self.lst.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.lst.setStyleSheet(
            "QListWidget{font-size:12px;border:1px solid #dee2e6;border-radius:6px;}"
            "QListWidget::item{padding:3px;}"
            "QListWidget::item:selected{background:#cce5ff;color:#000;}"
            "QListWidget::item:alternate{background:#f8f9fa;}")
        lay.addWidget(self.lst)
        self._refresh()

    # ── Android display toggle helpers ────────────────────────────
    def _on_android_screen_click(self):
        self.btn_android_screen.setChecked(True)
        self.btn_full_screen.setChecked(False)
        self.btn_android_screen.setStyleSheet(self._TOGGLE_ON)
        self.btn_full_screen.setStyleSheet(self._TOGGLE_OFF)

    def _on_full_screen_click(self):
        self.btn_full_screen.setChecked(True)
        self.btn_android_screen.setChecked(False)
        self.btn_full_screen.setStyleSheet(self._TOGGLE_ON)
        self.btn_android_screen.setStyleSheet(self._TOGGLE_OFF)

    def _on_cookie_toggle(self, checked):
        self.btn_collect_cookie.setText(f"🍪  Collect Cookie: {'ON' if checked else 'OFF'}")
        self.btn_collect_cookie.setStyleSheet(self._TOGGLE_ON if checked else self._TOGGLE_OFF)

    def _android_display(self):
        # CORRECTED mapping:
        # "Android Screen" button → phone-sized window (android_screen)
        # "Full Screen" button    → maximized window (full_screen)
        return "full_screen" if self.btn_full_screen.isChecked() else "android_screen"

    def _filter_profiles(self, query: str = ""):
        """Filter the saved profiles list by name or serial number."""
        q = query.strip().lower()
        for i in range(self.lst.count()):
            item = self.lst.item(i)
            pid  = item.data(Qt.ItemDataRole.UserRole) or ""
            meta = self._rows.get(pid)
            if meta is None:
                item.setHidden(False); continue
            row_pid   = pid.lower()
            row_name  = load_meta(pid).get("profile_name","").lower()
            row_serial= str(load_meta(pid).get("serial",""))
            visible = (not q or q in row_pid or q in row_name or q == row_serial or
                       q.lstrip("#") == row_serial)
            item.setHidden(not visible)

    # ── Status message with auto-fade ─────────────────────────────
    def _show_status(self, msg: str, color="#495057", timeout_ms=3000):
        self.lbl.setText(msg)
        self.lbl.setStyleSheet(f"color:{color};font-size:12px;font-weight:bold;min-height:20px;")
        self._lbl_timer.start(timeout_ms)

    # ── License ───────────────────────────────────────────────────
    def _on_license(self, data: dict):
        self._license = data
        status = data.get("status",""); rem = data.get("remaining_days",0)
        name   = data.get("name","")
        if name and status not in ("approved",): self.txt_name.setText(name)

        self.btn_renew.setVisible(False)

        if status == "approved":
            self.lbl_status.setText("✅  Active")
            self.lbl_status.setStyleSheet("color:#28a745;font-size:13px;font-weight:bold;")
            self.lbl_remaining.setText(f"⏱ {rem} days remaining")
            # Hide registration and login sections — only code + remaining stay
            for w in (self.txt_name, self.txt_phone, self.btn_submit,
                      self.txt_login_code, self.btn_login):
                w.setVisible(False)
            self._set_features(True)

            # ── Subscription renewal reminder ─────────────────────
            # If subscription_days > 7 → remind at 6 days remaining
            # If subscription_days ≤ 7 → remind at 3 days remaining
            sub_days = data.get("subscription_days", 30)
            remind_threshold = 6 if sub_days > 7 else 3
            if 0 < rem <= remind_threshold:
                self.btn_renew.setVisible(True)
                self.btn_renew.setText(f"🔄 {rem}d left — Send Renew Request")
                self.btn_renew.setStyleSheet(
                    "QPushButton{background:#dc3545;color:white;font-weight:bold;"
                    "border-radius:4px;padding:4px 12px;font-size:12px;}"
                    "QPushButton:hover{background:#b02a37;}"
                )
                # Auto-submit renew request if very close to expiry (1 day left)
                if rem <= 1 and not data.get("renew_auto_sent"):
                    self._auto_renew_request()
            else:
                self.btn_renew.setVisible(False)

        elif status == "device_mismatch":
            self.lbl_status.setText("⚠️  This license is active on another device. Enter its code to switch.")
            self.lbl_status.setStyleSheet("color:#fd7e14;font-size:12px;font-weight:bold;")
            self.lbl_remaining.setText(f"⏱ {rem} days (another device)")
            # Show login fields so user can re-claim
            for w in (self.txt_login_code, self.btn_login): w.setVisible(True)
            for w in (self.txt_name, self.txt_phone, self.btn_submit): w.setVisible(False)
            self._set_features(False)

        elif status == "expired":
            self.lbl_status.setText("⌛  Expired")
            self.lbl_status.setStyleSheet("color:#fd7e14;font-size:12px;font-weight:bold;")
            self.lbl_remaining.setText(""); self.btn_renew.setVisible(True)
            for w in (self.txt_name, self.txt_phone, self.btn_submit,
                      self.txt_login_code, self.btn_login):
                w.setVisible(False)
            self._set_features(False)

        elif status == "blocked":
            self.lbl_status.setText("🚫  Access blocked. Contact admin.")
            self.lbl_status.setStyleSheet("color:#dc3545;font-size:12px;font-weight:bold;")
            self.lbl_remaining.setText(f"({rem}d saved)" if rem else "")
            for w in (self.txt_name, self.txt_phone, self.btn_submit,
                      self.txt_login_code, self.btn_login):
                w.setVisible(False)
            self._set_features(False)

        elif status == "pending":
            self.lbl_status.setText("⏳  Wait for Approval…")
            self.lbl_status.setStyleSheet("color:#6c757d;font-size:12px;font-weight:bold;")
            for w in (self.txt_name, self.txt_phone, self.btn_submit): w.setVisible(False)
            for w in (self.txt_login_code, self.btn_login): w.setVisible(True)
            self._set_features(False)

        elif status == "not_found":
            self.lbl_status.setText("ℹ️  Enter your name & phone, then click Submit")
            self.lbl_status.setStyleSheet("color:#0078d7;font-size:12px;")
            for w in (self.txt_name, self.txt_phone, self.btn_submit,
                      self.txt_login_code, self.btn_login):
                w.setVisible(True)
            self._set_features(False)

        elif status == "no_firebase":
            self.lbl_status.setText("⚠️  Firebase unavailable (pip install firebase-admin)")
            self.lbl_status.setStyleSheet("color:#fd7e14;font-size:12px;")
            self._set_features(True)

        else:
            self.lbl_status.setText(f"⚠️  {status}")
            self._set_features(False)

    def _set_features(self, on: bool):
        """
        Enable or disable all create/launch features based on license status.
        When off: no new profiles, no launching existing profiles.
        Per-row Launch buttons are also disabled so no bypass is possible.
        """
        for w in (self.btn_create, self.btn_cl, self.btn_ls, self.btn_del, self.btn_select):
            w.setEnabled(on)
        # Also lock/unlock each profile row's Launch button
        for row in self._rows.values():
            if not on:
                # License invalid — grey out Launch, show why
                row.btn_launch.setEnabled(False)
                row.btn_launch.setStyleSheet(
                    "QPushButton{background:#adb5bd;color:white;font-weight:bold;"
                    "border-radius:5px;font-size:12px;}")
                row.btn_launch.setToolTip("License required to launch profiles")
            else:
                # License valid — restore button state
                if self._threads.get(row.pid) and self._threads[row.pid].isRunning():
                    row.set_running(True)
                else:
                    row._style_idle()
                    row.btn_launch.setToolTip("")

    def _submit_request(self):
        name=self.txt_name.text().strip(); phone=self.txt_phone.text().strip()
        if not name: QMessageBox.warning(self,"","Please enter your name."); return
        if not phone: QMessageBox.warning(self,"","Please enter your phone number."); return
        self.btn_submit.setEnabled(False); self.btn_submit.setText("Sending…")
        if submit_license_request(name, phone):
            self.lbl_status.setText("⏳  Wait for Approval…")
            self.lbl_status.setStyleSheet("color:#6c757d;font-size:12px;font-weight:bold;")
            for w in (self.txt_name, self.txt_phone, self.btn_submit): w.setVisible(False)
        else:
            self.lbl_status.setText("❌  Failed to send. Check internet connection.")
            self.btn_submit.setEnabled(True); self.btn_submit.setText("📤 Submit")

    def _send_renew(self):
        self.btn_renew.setEnabled(False)
        if submit_renew_request():
            self.lbl_status.setText("⏳  Wait for Approval…")
            self.lbl_status.setStyleSheet("color:#6c757d;font-size:12px;font-weight:bold;")
            self.btn_renew.setVisible(False)
        else:
            self.btn_renew.setEnabled(True)
            QMessageBox.warning(self,"","Failed to send renew request.")

    def _auto_renew_request(self):
        """Silently auto-submit renew when 1 day or less remains (no popup)."""
        try:
            if submit_renew_request():
                # Mark as auto-sent in Firebase so we don't spam
                db = get_db()
                if db:
                    code = get_session_code()
                    db.collection("users").document(code).update({
                        "renew_auto_sent": True,
                        "renew_auto_sent_at": fb_store.SERVER_TIMESTAMP,
                    })
        except Exception:
            pass

    def _login_by_code(self):
        code = self.txt_login_code.text().strip().upper()
        if not code:
            self._show_status("⚠️  Enter a code first","#fd7e14"); return
        self.btn_login.setEnabled(False); self.btn_login.setText("Checking…")
        self.lbl_status.setText("⏳ Verifying code…")
        self._claim_thread = ClaimLicenseThread(self, code)
        self._claim_thread.result.connect(self._on_claim_result)
        self._claim_thread.start()

    def _on_claim_result(self, ok: bool, msg: str):
        self.btn_login.setEnabled(True); self.btn_login.setText("🔐 Login")
        if ok:
            self._show_status("✅  Logged in successfully!","#28a745",4000)
            self._start_lic_check()   # re-check with new session code
        else:
            self._show_status(f"❌  {msg}","#dc3545",5000)

    def _do_refresh(self):
        """Refresh button — always works regardless of license status."""
        self._show_status("🔄 Refreshing…","#0078d7",3000)
        self._start_lic_check()
        self._refresh()

    # ── Select mode ───────────────────────────────────────────────
    def _toggle_select_mode(self, active):
        self._select_mode=active
        self.btn_select.setText("✅ ON" if active else "☑  Select")
        for row in self._rows.values(): row.set_select_mode(active)

    def _selected_pids(self):
        if self._select_mode: return [pid for pid,r in self._rows.items() if r.is_checked()]
        return [i.data(Qt.ItemDataRole.UserRole) for i in self.lst.selectedItems()]

    # ── Profile creation ──────────────────────────────────────────
    def _dev_mode(self):
        if self.rb_mobile.isChecked(): return "mobile"
        if self.rb_mixed.isChecked():  return "mixed"
        return "desktop"

    def _webrtc_mode(self):
        return "lock" if self.rb_webrtc_lock.isChecked() else "mask"

    def _network_mode(self):
        if self.rb_vpn.isChecked():   return "vpn"
        if self.rb_proxy.isChecked(): return "proxy"
        return "none"

    # ── Go ────────────────────────────────────────────────────────
    def _check_license_gate(self) -> bool:
        """
        Returns True only if the user has an active approved license on THIS device.
        Blocks: expired, not_found, device_mismatch, blocked, pending.
        Shows a clear message dialog so the user understands why.
        """
        status = self._license.get("status","")
        if status == "approved":
            return True
        messages = {
            "expired":         "⌛  Your subscription has expired.\nPlease contact admin to renew.",
            "blocked":         "🚫  Your account is blocked.\nPlease contact admin.",
            "device_mismatch": "⚠️  This license is active on another device.\nLogin with your code to switch.",
            "pending":         "⏳  Your request is pending admin approval.\nPlease wait.",
            "not_found":       "ℹ️  No active license found.\nEnter your name & phone and submit a request.",
            "no_firebase":     "⚠️  Firebase unavailable.\n(pip install firebase-admin)",
        }
        msg = messages.get(status, f"⚠️  License status: {status}\nPlease contact admin.")
        QMessageBox.warning(self, "Access Denied", msg)
        return False

    def _go(self, launch: bool):
        """Start profile creation — all network calls run in background thread."""
        if not self._check_license_gate(): return
        if self.rb_proxy.isChecked():
            proxies = [p.strip() for p in self.ptxt.toPlainText().strip().splitlines() if p.strip()]
            if not proxies:
                QMessageBox.warning(self,"","Please enter at least one proxy."); return
            # Basic format validation only (no network call on main thread)
            bad = []
            for p in proxies:
                px = parse_proxy(p)
                try:
                    port = int(px["port"])
                    if not (1 <= port <= 65535): raise ValueError
                except ValueError:
                    bad.append(f"Invalid port in: '{p}'")
            if bad:
                QMessageBox.critical(self,"Proxy Error","\n\n".join(bad)); return
        else:
            proxies = [""] * self.spin.value()

        opts = {
            "dm":             self._dev_mode(),
            "lang_mode":      "ip_based" if self.rb_lang_ip.isChecked() else "default",
            "webrtc_mode":    self._webrtc_mode(),
            "collect_cookie": self.btn_collect_cookie.isChecked(),
            "android_display":self._android_display(),
            "network_mode":   self._network_mode(),
        }

        for b in (self.btn_create, self.btn_cl): b.setEnabled(False)
        self._show_status(
            f"⏳ {len(proxies)} profile{'s' if len(proxies)>1 else ''} creating…",
            "#0078d7", 120000
        )

        # Run in background thread — no main-thread blocking, no EXE crash
        self._creation_thread = ProfileCreationThread(self, proxies, opts, launch)
        self._creation_thread.progress.connect(lambda msg: self._show_status(f"⏳ {msg}","#0078d7",30000))
        self._creation_thread.done.connect(self._on_profiles_created)
        self._creation_thread.error.connect(self._on_creation_error)
        self._creation_thread.start()

    def _on_profiles_created(self, created: list, launch: bool):
        self._refresh()
        n = len(created)
        self._show_status(f"✅ {n} profile{'s' if n>1 else ''} created.","#28a745",3000)
        for b in (self.btn_create, self.btn_cl): b.setEnabled(True)
        if launch:
            for pid, meta in created: self._launch_pid(pid, meta)

    def _on_creation_error(self, err: str):
        self._show_status(f"❌ {err[:70]}","#dc3545",5000)
        for b in (self.btn_create, self.btn_cl): b.setEnabled(True)
        QMessageBox.critical(self,"Profile Creation Error", err)

    # ── Launch ────────────────────────────────────────────────────
    def _launch_pid(self, pid, meta=None):
        if not self._check_license_gate(): return
        if pid in self._threads and self._threads[pid].isRunning():
            self._show_status(f"⚠️  '{pid}' already running","#fd7e14"); return
        if meta is None:
            m=load_meta(pid); fp=m.get("fingerprint")
            if not fp: QMessageBox.warning(self,"Error",f"Fingerprint missing — recreate: {pid}"); return
            meta={"fingerprint":fp,"proxy":m.get("proxy",""),"timezone":m.get("timezone","UTC"),
                  "webrtc_mode":m.get("webrtc_mode","mask"),"android_display":m.get("android_display","android_screen")}
        proxy=meta.get("proxy","")
        # ── Proxy format check ONLY on main thread (safe, instant) ──
        # Full connectivity check runs inside BrowserThread._launch()
        # to avoid blocking/freezing the main thread on EXE builds.
        if proxy:
            px = parse_proxy(proxy)
            try:
                port = int(px["port"])
                if not (1 <= port <= 65535):
                    raise ValueError
            except ValueError:
                QMessageBox.critical(self,"Proxy Error",
                    f"Invalid proxy port: '{proxy}'\nExpected: IP:Port or IP:Port:User:Pass")
                return
        t=BrowserThread(pid,meta)
        t.launched.connect(self._on_launched); t.error.connect(self._on_error)
        t.browser_closed.connect(self._on_closed)
        self._threads[pid]=t; t.start()
        self._show_status(f"⏳ Launching: {pid}…","#0078d7",5000)
        if pid in self._rows: self._rows[pid].set_running(True)

    def _launch_selected(self):
        for pid in self._selected_pids(): self._launch_pid(pid)

    def _delete_selected(self):
        pids=self._selected_pids()
        if not pids: return
        running=[p for p in pids if p in self._threads and self._threads[p].isRunning()]
        msg=f"Delete {len(pids)} profile(s) permanently?"
        if running: msg+=f"\n\n⚠️  {len(running)} currently running — will be closed."
        if QMessageBox.question(self,"Confirm Delete",msg,
               QMessageBox.StandardButton.Yes|QMessageBox.StandardButton.No)!=QMessageBox.StandardButton.Yes: return
        for pid in pids:
            if pid in self._threads: self._threads[pid].stop(); del self._threads[pid]
            remove_serial(pid)
            p=os.path.join(PROFILES_DIR,pid)
            if os.path.exists(p): shutil.rmtree(p,ignore_errors=True)
        self._refresh()

    def _rename_profile(self, pid):
        meta=load_meta(pid)
        name,ok=QInputDialog.getText(self,"Rename Profile",f"New name for:\n{pid}",text=meta.get("profile_name",""))
        if ok and name.strip(): update_meta_name(pid,name.strip()); self._refresh()

    # ── Cookie collection ─────────────────────────────────────────
    def _enable_cookie_for_profile(self, pid: str):
        """Retroactively enable cookie collection on a profile that was created without it."""
        update_meta_cookie(pid, True)
        self._refresh()
        self._show_status(f"🍪 Cookie collection enabled for {pid}","#28a745",3000)

    def _all_cookie_req(self, pid):
        """Works whether browser is running OR already closed (reads from disk cache)."""
        cookies = []
        t = self._threads.get(pid)
        if t and t.isRunning():
            cookies = t.get_all_cookies()
        else:
            cf = os.path.join(PROFILES_DIR, pid, "_titan_cookies.json")
            if os.path.exists(cf):
                try:
                    with open(cf, encoding="utf-8") as f: cookies = json.load(f)
                except Exception: cookies = []
        if not cookies:
            self._show_status("ℹ️  No cookies — login to a site first","#fd7e14",3000); return
        dlg = CookieDialog(f"All Cookies — {pid}", cookies, self); dlg.exec()

    def _site_cookie_req(self, pid):
        """Works whether browser is running OR already closed."""
        cookies = []
        t = self._threads.get(pid)
        if t and t.isRunning():
            cookies = t.get_all_cookies()
        else:
            cf = os.path.join(PROFILES_DIR, pid, "_titan_cookies.json")
            if os.path.exists(cf):
                try:
                    with open(cf, encoding="utf-8") as f: cookies = json.load(f)
                except Exception: cookies = []
        if not cookies:
            self._show_status("ℹ️  No cookies — login to a site first","#fd7e14",3000); return
        domains = sorted({c.get("domain","").lstrip(".") for c in cookies if c.get("domain")})
        if not domains:
            self._show_status("ℹ️  No site cookies found","#fd7e14",3000); return
        class _CookieSource:
            def __init__(self, c): self._c = c
            def get_cookie_domains(self):
                return sorted({x.get("domain","").lstrip(".") for x in self._c if x.get("domain")})
            def get_cookies_for_domain(self, domain):
                return [x for x in self._c if domain in x.get("domain","")]
        dlg = SiteCookieDialog(domains, _CookieSource(cookies), self); dlg.exec()

    # ── Thread callbacks ──────────────────────────────────────────
    def _on_launched(self, pid):
        self._show_status(f"✅  Running: {pid}","#28a745",4000)
        if pid in self._rows: self._rows[pid].set_running(True)

    def _on_error(self, pid, err):
        self._show_status(f"❌  {err[:70]}","#dc3545",5000)
        if pid in self._threads: del self._threads[pid]
        if pid in self._rows:    self._rows[pid].set_running(False)
        tips=[]
        e=err.lower()
        if "winerror 32" in e or "being used by" in e: tips.append("🔒 WinError 32 — launch profiles one at a time.")
        if "proxy" in e or "err_proxy" in e: tips.append("🔌 Proxy unreachable — check proxy or use None mode.")
        if "version" in e or "chromedriver" in e: tips.append("🔄 Chrome mismatch: pip install -U undetected-chromedriver")
        if "permission" in e or "access" in e: tips.append("🔒 Permission error — Run as Administrator.")
        if not tips: tips.append("💡 Try None mode without proxy.")
        QMessageBox.critical(self,"Launch Error",f"Profile: {pid}\n\n{err}\n\n"+"\n".join(tips))

    def _on_closed(self, pid):
        self._show_status(f"🔒  Closed: {pid}","#6c757d",3000)
        if pid in self._threads: del self._threads[pid]
        if pid in self._rows:    self._rows[pid].set_running(False)
        self._purge()

    # ── Refresh list ──────────────────────────────────────────────
    def _refresh(self):
        self.lst.clear(); self._rows.clear(); self._items.clear()
        if not os.path.exists(PROFILES_DIR): return
        entries=[]
        for p in os.listdir(PROFILES_DIR):
            pp=os.path.join(PROFILES_DIR,p)
            if not os.path.isdir(pp): continue
            meta=load_meta(p)
            if not meta: continue
            entries.append((meta.get("serial",0),p,meta))
        entries.sort(key=lambda x:x[0])
        for _,p,meta in entries:
            row=ProfileRow(p,meta)
            row.launch_requested.connect(self._launch_pid)
            row.rename_requested.connect(self._rename_profile)
            row.all_cookie_req.connect(self._all_cookie_req)
            row.site_cookie_req.connect(self._site_cookie_req)
            row.enable_cookie_req.connect(self._enable_cookie_for_profile)
            row.set_select_mode(self._select_mode)
            item=QListWidgetItem(self.lst)
            item.setData(Qt.ItemDataRole.UserRole,p)
            item.setSizeHint(row.sizeHint())
            self.lst.addItem(item); self.lst.setItemWidget(item,row)
            self._rows[p]=row; self._items[p]=item
            if p in self._threads and self._threads[p].isRunning():
                row.set_running(True)


# ══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    # Required for PyInstaller EXE — prevents child process re-launch crash
    multiprocessing.freeze_support()
    app=QApplication(sys.argv); app.setStyle("Fusion")
    win=TitanAntiDetect(); win.show(); sys.exit(app.exec())
