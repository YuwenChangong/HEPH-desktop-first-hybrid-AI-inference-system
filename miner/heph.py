import sys, time, platform, subprocess, ollama, uuid, os, base64, requests, hashlib, logging, threading, queue, multiprocessing, re
import json
from PIL import Image
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from dotenv import load_dotenv, find_dotenv
from supabase import create_client
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Miner runtime logging
logging.basicConfig(
    filename='miner.log',
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    encoding='utf-8'
)

MOJIBAKE_MARKERS = ("闂", "婵", "濠", "锟", "鈥", "顭", "鐎", "閻", "缂", "鏄", "鍊")
ASCII_CHUNK_RE = re.compile(r"[A-Za-z0-9_/\-\[\]\(\)\{\}\.:,=+| ]{4,}")


def _looks_mojibake(text: str) -> bool:
    return any(marker in text for marker in MOJIBAKE_MARKERS)


def _sanitize_log_message(text: str) -> str:
    chunks = [chunk.strip() for chunk in ASCII_CHUNK_RE.findall(text or "") if chunk.strip()]
    if not chunks:
        return "[Miner] 运行日志事件"

    deduped = []
    for chunk in chunks:
        if not deduped or deduped[-1] != chunk:
            deduped.append(chunk)
    preview = " | ".join(deduped[:8])
    return f"[Miner] 日志清洗: {preview}"


def log(msg):
    safe_msg = str(msg)
    if _looks_mojibake(safe_msg):
        safe_msg = _sanitize_log_message(safe_msg)
    print(safe_msg)
    logging.info(safe_msg)

def _safe_load_env():
    """
    Load .env without crashing in packaged/frozen or non-standard launch contexts.
    """
    candidates = []
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        if base_dir:
            candidates.append(os.path.join(base_dir, ".env"))
    except Exception:
        pass

    cwd = os.getcwd()
    if cwd:
        candidates.append(os.path.join(cwd, ".env"))

    seen = set()
    for path in candidates:
        norm = os.path.normcase(os.path.abspath(path))
        if norm in seen:
            continue
        seen.add(norm)
        try:
            if os.path.exists(path):
                load_dotenv(dotenv_path=path, override=False)
                log(f"[Miner] Loaded env from: {path}")
                return
        except Exception as e:
            log(f"[Miner] WARN failed to load env file {path}: {e}")

    # Final fallback: find_dotenv with usecwd to avoid frame assertion errors.
    try:
        fallback = find_dotenv(usecwd=True)
        if fallback:
            load_dotenv(dotenv_path=fallback, override=False)
            log(f"[Miner] Loaded env from fallback: {fallback}")
        else:
            log("[Miner] .env not found; using process environment only")
    except Exception as e:
        log(f"[Miner] WARN dotenv auto-discovery skipped: {e}")


_safe_load_env()


def get_env(*names):
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


GATEWAY_URL = get_env("HEPH_GATEWAY_URL", "GATEWAY_URL")
ACCESS_TOKEN = get_env("HEPH_ACCESS_TOKEN", "ACCESS_TOKEN")
SUPABASE_URL = get_env("HEPH_SUPABASE_URL", "SUPABASE_URL")
SUPABASE_KEY = get_env("HEPH_SUPABASE_KEY", "SUPABASE_KEY")

IMAGE_MAX_BYTES = 5 * 1024 * 1024
STREAM_RESULT_DELTA_MAX_LEN = 12000
GATEWAY_CONNECT_TIMEOUT = 5
GATEWAY_READ_TIMEOUT = 25
OLLAMA_READ_TIMEOUT_SECONDS = int(get_env("HEPH_OLLAMA_READ_TIMEOUT_SECONDS", "OLLAMA_READ_TIMEOUT_SECONDS") or "600")
LOCAL_PROFILE_HOST = get_env("HEPH_LOCAL_PROFILE_HOST", "LOCAL_PROFILE_HOST") or "127.0.0.1"
LOCAL_PROFILE_PORT = int(get_env("HEPH_LOCAL_PROFILE_PORT", "LOCAL_PROFILE_PORT") or "8765")
AUTO_CLAIM_DEFAULT = str(get_env("HEPH_AUTO_CLAIM_DEFAULT", "AUTO_CLAIM_DEFAULT") or "0").strip().lower() in ("1", "true", "yes", "on")


def get_int_env(*names, default: int, minimum: int = 1) -> int:
    raw_value = get_env(*names)
    if raw_value is None:
        return default
    try:
        return max(minimum, int(raw_value))
    except ValueError:
        log(f"闂傚倸鍊搁崐鎼佸磹妞嬪海鐭嗗〒姘ｅ亾闁诡喖娼″畷鎯邦槷闁哄鐗犻弻锟犲炊閳轰焦鐎婚梺鎼炲妽濡啴骞冨Δ鍛棃婵炴垶鐟﹂崰鎰磽?闂傚倸鍊搁崐鎼佸磹閻戣姤鍤勯柛鎾茬閸ㄦ繃銇勯弽銊х煀闁搞劍绻堥弻锝呂熷▎鎯ф闂佹悶鍔岄崐鎼佹箒闂佺绻楅崑鎰板汲濮椻偓閺屽秷顧侀柛鎾寸懇瀹曟劙骞愭惔婵堢畾闂佸綊妫跨粈渚€鎮″☉妯忓綊鏁愰崼鐕佷哗闂佸憡锕╅崜姘辨崲濠靛棌鏋旈柛顭戝枟閻忓秹姊洪棃娑欏闁烩晩鍨伴悾鐑藉箣閿曗偓缁犺崵绱撴担璇＄劷闁?{names[0]}={raw_value} 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵稿妽闁稿顑夐悡顐﹀炊閵娧€妲堢紒鐐劤椤兘寮婚敐澶婄疀妞ゆ帒鍊风划鐢告⒑缁嬭法绠茬紒顔芥崌瀵鈽夐姀鈺傛櫇闂佹寧绻傚Λ娑⑺囬妸鈺傗拺闁告繂瀚刊濂告煕閹捐泛鏋涙鐐插暙椤粓鍩€椤掑嫬绠栨繛鍡樺灦瀹曞鏌ｉ埡鍌氱瑲闁圭澧藉Σ鎰板箳濡も偓閻掑灚銇勯幒鎴濐仼闁绘帒鐏氶妵鍕箳閹存繍浠鹃梺鎶芥敱鐢帡濡撮幒鎴僵妞ゆ帊鐒﹂幃娆愮箾鐎涙鐭嬮柛鏃€鐟╁璇测槈濮橈絽浜鹃柨婵嗛娴滄繈鎮樿箛锝呭籍闁哄苯绉归幐濠冨緞濡儵鏋呮俊銈囧Х閸嬬偤鈥﹂崶顒€鐒垫い鎺嶈兌閳洟鎳ｈ闇夐柣姗嗗枛閻忣喚绱掓潏銊﹀鞍缂佹鍠栧畷鎯邦槼闁绘繍鍣ｅ娲箹閻愭彃顬夋繝鐢靛仜閿曨亪鎮伴璺ㄧ杸闁哄洦顨呮禍楣冩煥濠靛棝顎楅柡瀣洴瀹曞爼骞橀瑙ｆ嫼闂佸憡绋戦敃锝囨閸楃伝鐟邦煥閸曨厾鐓佺紓?{default}")
        return default


def get_bool_env(*names, default: bool = False) -> bool:
    raw_value = get_env(*names)
    if raw_value is None:
        return default
    return str(raw_value).strip().lower() in ("1", "true", "yes", "on")


DEFAULT_SYSTEM_PROMPT = """You are a precise and practical AI assistant.
Respond in the user's language by default.
For coding tasks, prioritize correct, runnable code and follow explicit output format requirements exactly.
Do not include chain-of-thought, hidden reasoning, or extra analysis unless the user explicitly asks for it.
If the user asks for code only, return code only.
If you are unsure, say what is uncertain instead of inventing details.
"""


def load_system_prompt() -> str:
    prompt_file = get_env("HEPH_SYSTEM_PROMPT_FILE", "SYSTEM_PROMPT_FILE")
    if prompt_file:
        try:
            with open(prompt_file, "r", encoding="utf-8") as f:
                text = f.read().strip()
                if text:
                    return text
        except Exception as e:
            log(f"WARN failed to load system prompt file {prompt_file}: {e}")

    prompt_inline = get_env("HEPH_SYSTEM_PROMPT", "SYSTEM_PROMPT")
    if prompt_inline:
        return prompt_inline.replace("\\n", "\n").strip()

    return DEFAULT_SYSTEM_PROMPT


def build_gateway_session():
    retry = Retry(
        total=2,
        connect=2,
        read=2,
        status=2,
        backoff_factor=1,
        allowed_methods=frozenset(["POST"]),
        status_forcelist=[429, 500, 502, 503, 504],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


GATEWAY_SESSION = build_gateway_session()

supabase = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        log(f"闂傚倸鍊搁崐鎼佸磹妞嬪海鐭嗗〒姘ｅ亾闁诡喖娼″畷鎯邦槷闁哄鐗犻弻锟犲炊閳轰焦鐎婚梺鎼炲妽濡啴骞冨Δ鍛棃婵炴垶鐟﹂崰鎰磽?Supabase 闂傚倸鍊搁崐鎼佸磹妞嬪海鐭嗗〒姘ｅ亾妤犵偛顦甸弫宥夊礋椤掍焦顔囨繝寰锋澘鈧洟宕姘辨殾闁哄被鍎查悡鏇犫偓鍏夊亾闁逞屽墴瀹曟洟骞嬮悩鐢殿槸闂佸搫绋侀崢浠嬫偂濞嗘挻鐓熸俊銈傚亾闁绘锕﹀▎銏ゆ嚑椤掑倻锛滈梺閫炲苯澧柣锝嗙箞瀹曠喖顢楅崒姘闂佽姘﹂～澶愬箹閳哄懎绐楅柡鍥╁剱濞撳鏌熼悜姗嗘當缂佺嫏鍕╀簻闁规儳宕悘顏堟煟? {e}")

stream_update_queue = queue.Queue(maxsize=100)
stream_update_worker = None

MINER_NAME = get_env("HEPH_MINER_NAME", "MINER_NAME")
if not MINER_NAME:
    MINER_NAME = f"miner-{uuid.uuid4().hex[:8]}"
    with open(".env", "a") as f:
        f.write(f"\nMINER_NAME={MINER_NAME}")
    log(f"婵犵數濮烽。钘壩ｉ崨鏉戠；闁规崘娉涚欢銈吤归悩宸剰闁汇値鍠楅妵鍕箛閸撲胶鏆犻梺?闂傚倸鍊搁崐鎼佸磹妞嬪海鐭嗗〒姘ｅ亾妤犵偞鐗犻、鏇㈡晝閳ь剛澹曡ぐ鎺撳仭婵炲棗绻愰顐︽煟濠靛棛鍩ｉ柡灞剧洴閸╁嫰宕橀浣割潓闂備胶顭堥鍛偓姘嵆瀵鈽夐姀鐘靛姶闂佸憡鍔楅崑鎾活敇瑜版帗鈷戦柛婵勫劚閺嬪酣鏌熼搹顐€跨€殿喖顭峰鎾偄妞嬪海鐛梻浣稿閸嬪懐鎹㈤崒娑欏弿閹兼番鍔嶉埛鎴︽煙閼测晛浠滈柍褜鍓氶悧婊堝极椤斿槈鏃堝礃閳轰礁绨ユ繝鐢靛仦閸ㄥ爼鎮烽妷褎顐介柣鎰劋閻撴洘銇勯幇鍓佹偧缂佺姵鎸婚妵鍕敆閳ь剟鎮ч幘鎰佹綎婵炲樊浜滄导鐘绘煏婢跺牆鍔欑紒顔ㄥ懐纾藉〒姘搐閺嬬喖鏌ｉ悢鍙夋珔妞ゆ洩缍侀、鏇㈠閳轰焦鍊梻浣规偠閸庤崵寰婇懞銉ь洸闁绘劦鍓涚粻楣冩煙鐎电浠ч柟鍐插缁? {MINER_NAME}")

if not GATEWAY_URL or not GATEWAY_URL.startswith("http"):
    log("闂?闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸婂潡鏌ㄩ弮鍫熸殰闁稿鎸剧划顓炩槈濡顦╅梺绋款儜缁绘繈寮婚弴鐔虹闁绘劦鍓氶悵锕傛⒑? .env 濠电姷鏁告慨鐑藉极閹间礁纾婚柣鎰惈閸ㄥ倿鏌涢锝嗙缂佺姴缍婇弻宥夊传閸曨剙娅ｉ梺绋胯閸旀垿寮婚敃鈧灒濞撴凹鍨遍鍡涙⒑閼测晛顣奸悗绗涘洤桅闁告洦鍨扮粻濠氭煕濡ゅ啫浠уù鐘荤畺濮婅櫣绱掑Ο璇茬殤闂佺顑嗛幑鍥蓟閿濆棙鍎熼柨娑樺閺嗘盯姊虹粙鍧楊€楃€规洦鍓涢崣鍛存⒑闂堟单鍫ュ疾濞戙垹绀勯柣妯肩帛閻撴洟鎮橀悙鎻掆挃闁愁垱娲熼弻娑樷枎韫囨洜顔夌紓浣虹帛缁诲牆鐣疯ぐ鎺濇晝闁挎繂鎳忓▓顐︽⒒娴ｇ瓔鍤冮柛鐘崇墵婵″爼骞栨担姝屾憰閻庡箍鍎遍ˇ顖氭暜闂備焦瀵уΛ浣圭珶閸儱鐒垫い鎺嗗亾婵炵》绻濆?GATEWAY_URL")
    sys.exit(1)


def calculate_hash(text):
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def get_hwid():
    def run_cmd(cmd):
        try:
            return subprocess.check_output(cmd, shell=True).decode().strip()
        except:
            return ""

    if platform.system() == "Windows":
        baseboard = run_cmd("wmic baseboard get serialnumber").split('\n')[-1].strip()
        cpu = run_cmd("wmic cpu get processorid").split('\n')[-1].strip()
        disk = run_cmd("wmic diskdrive get serialnumber").split('\n')[-1].strip()
        raw_data = f"{baseboard}|{cpu}|{disk}"
    else:
        raw_data = str(uuid.getnode())
    return hashlib.sha256(raw_data.encode()).hexdigest()


def get_gpu_info():
    vram_gb = 0
    gpu_count = 1
    try:
        try:
            cmd = "nvidia-smi --query-gpu=memory.total --format=csv,noheader,nounits"
            output = subprocess.check_output(cmd, shell=True).decode().strip()
            vram_gb = max([int(x) for x in output.split('\n')]) / 1024
            gpu_count = len(output.split('\n'))
            if vram_gb > 0: return vram_gb, gpu_count
        except:
            pass

        import wmi
        c = wmi.WMI()
        for gpu in c.Win32_VideoController():
            raw_ram = gpu.AdapterRAM
            ram_fixed = (raw_ram + 2 ** 32) if raw_ram < 0 else raw_ram
            vram_curr = ram_fixed / (1024 ** 3)
            if 3.8 <= vram_curr <= 4.2:
                if any(x in gpu.Name.upper() for x in ["RTX", "RX", "ARC", "GTX"]):
                    vram_curr = 8.0
            vram_gb = max(vram_gb, vram_curr)
    except:
        vram_gb = 4.0
    return vram_gb, gpu_count


def get_hardware_config():
    vram_gb, gpu_count = get_gpu_info()
    force_vram = get_env("HEPH_FORCE_VRAM", "FORCE_VRAM")
    if force_vram:
        try:
            vram_gb = float(force_vram)
            log(f"闂傚倸鍊搁崐鎼佸磹妞嬪海鐭嗗〒姘ｅ亾闁诡喖娼″畷鎯邦槷闁哄鐗犻弻锟犲炊閳轰焦鐎婚梺鎼炲妽濡啴骞冨Δ鍛棃婵炴垶鐟﹂崰鎰磽?濠电姷鏁告慨鐑藉极閹间礁纾婚柣妯款嚙缁犲灚銇勮箛鎾搭棤缂佲偓婵犲洦鐓冪憸婊堝礈濮樿鲸宕叉繛鎴炵懃缁剁偤鎮楅敐搴′簽妞わ缚鍗抽幃妤€鈻撻崹顔界彯闂佺顑呴敃銉︾┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤悗姘卞厴瀹曘儳鈧綆鍠楅埛鎴︽偣閸ャ劌绲婚柛銈堜含缁辨挸顓奸崨顕呮！缂備礁鍊哥粔鎾€﹂妸鈺佸耿闁冲搫鍊圭欢浼存⒒閸屾瑦绁版い鏇嗗應鍋撳☉鎺撴珚妞ゃ垺鐟╁浠嬵敇閻愮數宕堕梻浣告惈缁嬩線宕㈤懖鈺冧笉閻熸瑥瀚ㄦ禍婊堟煙閺夊灝顣抽柣锝変憾閺岋繝宕熼埡浣稿婵烇絽娲ら敃顏呬繆閸洖宸濇い鏂垮悑椤忕娀姊绘担鍛婂暈闁荤喆鍎甸弫鍐敂閸繆鎽曢梺缁樻⒒閳峰牓寮澶嬬厱闁斥晛鍠氬▓鏇㈡煕閿濆棙銇濋柡宀€鍠栭獮鎾诲箳鐎ｎ亙鍒掓繝娈垮枛閿曪妇鍒掗鐐茬闁告侗鍨遍崰鍡涙煕閺囥劌骞栭柛銈嗗笧缁辨捇宕掑▎鎴М濡炪倧瀵岄崹鍫曞箖閵夛妇闄勭紒瀣硶閻? {vram_gb}GB")
        except:
            pass

    log(f"婵犵數濮烽。钘壩ｉ崨鏉戠；闁规崘娉涚欢銈吤归悩宸剰闁汇値鍠楅妵鍕冀椤愵澀绮剁紓?闂傚倸鍊搁崐鎼佸磹妞嬪海鐭嗗〒姘ｅ亾妤犵偛顦甸弫鎾绘偐椤旂懓浜鹃柛鎰靛枛瀹告繃銇勯弽銊р槈閹兼潙锕ら埞鎴炲箠闁稿﹥娲熼獮蹇曗偓锝庡枛閺嬩礁鈹戦悩鍙夊闁绘挻娲樼换娑㈠箣濠靛棜鍩為梺璇茬箳閸犳劗鎹㈠☉妯兼殕濠电姳绶氶崑妤呮⒑閸濆嫭婀扮紒瀣尰缁傛帡鏁冮崒姘辩暰閻熸粍绮岃灋婵犲﹤鐗婇埛鎴︽⒑椤愩倕浠滈柤娲诲灡閺呭爼顢涢悙瀵稿幗濠德板€撶欢鈥斥枔濡偐纾兼い鏃傗拡閸庢梹顨ラ悙鏉戠瑨閾绘牕霉閿濆懏鍟為柣銈呭€垮濠氬磼濮橆兘鍋撻幖浣瑰亱濠电姴娲ょ粈鍡涙煃瑜滈崜鐔煎蓟閻旂厧绀堢憸蹇曟暜濞戙垺鐓? 闂傚倸鍊搁崐鎼佸磹妞嬪海鐭嗗〒姘ｅ亾妤犵偞鐗犻、鏇㈡晜閽樺缃曢梻浣告啞閸旓箓宕伴弽顐㈩棜濠电姵纰嶉悡娆撴煙椤栧棗鍠氶弳銏㈢磼閻愵剙绀冩俊顐㈠濠€渚€姊洪幐搴ｇ畵闁瑰啿绻橀獮澶岀矙濞嗗墽鍞甸悷婊冮鐓ら柣鏃傚帶閽冪喐绻涢幋娆忕仼閹喖鎮峰鍐€楅柍璇茬Ч瀵挳濮€閿涘嫬甯楅柣鐔哥矋缁挸鐣峰鍐ｆ斀閻庯絽澧庣粙蹇旂節閵忥絾纭鹃柤娲诲灦瀵悂宕奸埗鈺佷壕妤犵偛鐏濋崝姘舵煙閸欏鑰块柟顔哄灮缁瑧鎹勯妸褏宓侀梻浣筋嚙缁绘帡宕戦悢灏栤偓锕傚醇閻斾警娲?{vram_gb:.2f}GB")

    if vram_gb >= 16:
        return "FLAGSHIP", {"cod": "qwen3.5:27b", "score": 3}, gpu_count
    if vram_gb >= 6:
        return "STANDARD", {"cod": "qwen3.5:9b", "score": 2}, gpu_count
    return "LIGHTWEIGHT", {"cod": "qwen3.5:2b", "score": 1}, gpu_count


def derive_model_capability(vram_gb):
    try:
        vram = float(vram_gb or 0)
    except (TypeError, ValueError):
        vram = 0.0
    if vram >= 16:
        return {"score": 3, "label": "qwen3.5:27b"}
    if vram >= 6:
        return {"score": 2, "label": "qwen3.5:9b"}
    return {"score": 1, "label": "qwen3.5:2b"}


HARDWARE_LEVEL, MODEL_SET, GPU_COUNT = get_hardware_config()
HARDWARE_ID = get_hwid()
VRAM_GB = round(get_gpu_info()[0], 1)
LOCAL_PROFILE_CAPABILITY = derive_model_capability(VRAM_GB)
LOW_VRAM_MODE = get_bool_env("HEPH_LOW_VRAM_MODE", "LOW_VRAM_MODE", default=(VRAM_GB <= 6.5))
FORCE_NON_STREAM = get_bool_env("HEPH_FORCE_NON_STREAM", "FORCE_NON_STREAM", default=False)
MAX_TASK_RUNTIME = get_int_env("HEPH_MAX_TASK_RUNTIME", "MAX_TASK_RUNTIME", default=900, minimum=30)
STREAM_UPDATE_INTERVAL = float(get_env("HEPH_STREAM_UPDATE_INTERVAL", "STREAM_UPDATE_INTERVAL") or "0.12")
STREAM_UPDATE_MIN_DELTA = get_int_env("HEPH_STREAM_UPDATE_MIN_DELTA", "STREAM_UPDATE_MIN_DELTA", default=8, minimum=1)
OLLAMA_NUM_PREDICT = get_int_env(
    "HEPH_OLLAMA_NUM_PREDICT",
    "OLLAMA_NUM_PREDICT",
    default=(384 if LOW_VRAM_MODE else 1024),
    minimum=64
)
OLLAMA_NORMAL_NUM_PREDICT = get_int_env(
    "HEPH_OLLAMA_NORMAL_NUM_PREDICT",
    "OLLAMA_NORMAL_NUM_PREDICT",
    default=min(512, OLLAMA_NUM_PREDICT),
    minimum=64
)
OLLAMA_NUM_CTX = get_int_env(
    "HEPH_OLLAMA_NUM_CTX",
    "OLLAMA_NUM_CTX",
    default=(1024 if LOW_VRAM_MODE else 2048),
    minimum=256
)
OLLAMA_NUM_BATCH = get_int_env(
    "HEPH_OLLAMA_NUM_BATCH",
    "OLLAMA_NUM_BATCH",
    default=(32 if LOW_VRAM_MODE else 64),
    minimum=1
)
PROMPT_CHAR_LIMIT = get_int_env(
    "HEPH_PROMPT_CHAR_LIMIT",
    "PROMPT_CHAR_LIMIT",
    default=(1200 if LOW_VRAM_MODE else 3000),
    minimum=200
)
INFERENCE_RETRIES = get_int_env(
    "HEPH_INFERENCE_RETRIES",
    "INFERENCE_RETRIES",
    default=(2 if LOW_VRAM_MODE else 1),
    minimum=0
)
INFERENCE_WORKERS = get_int_env("HEPH_INFERENCE_WORKERS", "INFERENCE_WORKERS", default=1)
CLAIM_QUEUE_MAXSIZE = get_int_env("HEPH_CLAIM_QUEUE_SIZE", "CLAIM_QUEUE_SIZE", default=max(1, INFERENCE_WORKERS))
CLAIM_IDLE_SLEEP = get_int_env("HEPH_CLAIM_IDLE_SLEEP", "CLAIM_IDLE_SLEEP", default=5)
CLAIM_QUEUE_FULL_SLEEP = get_int_env("HEPH_CLAIM_QUEUE_FULL_SLEEP", "CLAIM_QUEUE_FULL_SLEEP", default=1)
RUNTIME_ERROR_SLEEP = get_int_env("HEPH_RUNTIME_ERROR_SLEEP", "RUNTIME_ERROR_SLEEP", default=10)

auto_claim_enabled = AUTO_CLAIM_DEFAULT
auto_claim_lock = threading.Lock()


def is_auto_claim_enabled() -> bool:
    with auto_claim_lock:
        return bool(auto_claim_enabled)


def set_auto_claim_enabled(value: bool):
    global auto_claim_enabled
    with auto_claim_lock:
        auto_claim_enabled = bool(value)
COMPLETION_QUEUE_MAXSIZE = get_int_env("HEPH_COMPLETION_QUEUE_SIZE", "COMPLETION_QUEUE_SIZE", default=max(1, INFERENCE_WORKERS))
OUTSTANDING_TASK_LIMIT = get_int_env(
    "HEPH_OUTSTANDING_TASK_LIMIT",
    "OUTSTANDING_TASK_LIMIT",
    default=max(1, INFERENCE_WORKERS)
)
MP_CONTEXT = multiprocessing.get_context("spawn")
ALLOW_ANY_INSTALLED_MODEL = get_bool_env(
    "HEPH_ALLOW_ANY_INSTALLED_MODEL",
    "ALLOW_ANY_INSTALLED_MODEL",
    default=True
)
EXTRA_ALLOWED_MODELS_RAW = get_env("HEPH_EXTRA_ALLOWED_MODELS", "EXTRA_ALLOWED_MODELS") or ""
EXTRA_ALLOWED_MODELS = {
    item.strip() for item in EXTRA_ALLOWED_MODELS_RAW.split(",") if item.strip()
}
ACCEPTED_SOURCES_RAW = get_env("HEPH_ACCEPTED_SOURCES", "ACCEPTED_SOURCES") or ""
EXCLUDED_SOURCES_RAW = get_env("HEPH_EXCLUDED_SOURCES", "EXCLUDED_SOURCES") or ""
ACCEPTED_SOURCES = [item.strip() for item in ACCEPTED_SOURCES_RAW.split(",") if item.strip()]
EXCLUDED_SOURCES = [item.strip() for item in EXCLUDED_SOURCES_RAW.split(",") if item.strip()]
INSTALLED_OLLAMA_MODELS = set()
SYSTEM_PROMPT = load_system_prompt()
MAX_CONTINUATIONS = get_int_env("HEPH_MAX_CONTINUATIONS", "MAX_CONTINUATIONS", default=6, minimum=0)
MAX_STANDARD_CONTINUATIONS = get_int_env("HEPH_MAX_STANDARD_CONTINUATIONS", "MAX_STANDARD_CONTINUATIONS", default=0, minimum=0)
MAX_OUTPUT_CHARS = get_int_env("HEPH_MAX_OUTPUT_CHARS", "MAX_OUTPUT_CHARS", default=24000, minimum=500)
CODE_WORKFLOW_ENABLED = get_bool_env("HEPH_CODE_WORKFLOW_ENABLED", "CODE_WORKFLOW_ENABLED", default=True)
CODE_REVIEW_MIN_ISSUES = get_int_env("HEPH_CODE_REVIEW_MIN_ISSUES", "CODE_REVIEW_MIN_ISSUES", default=3, minimum=1)
CODE_MAX_STAGE_CHARS = get_int_env("HEPH_CODE_MAX_STAGE_CHARS", "CODE_MAX_STAGE_CHARS", default=6000, minimum=500)
CODE_HISTORY_CHARS = get_int_env("HEPH_CODE_HISTORY_CHARS", "CODE_HISTORY_CHARS", default=2400, minimum=400)

claimed_task_queue = queue.Queue(maxsize=CLAIM_QUEUE_MAXSIZE)
completion_queue = queue.Queue(maxsize=COMPLETION_QUEUE_MAXSIZE)
task_slot_semaphore = threading.BoundedSemaphore(OUTSTANDING_TASK_LIMIT)
inflight_tasks_lock = threading.Lock()
inflight_tasks = {}


class TaskTimeoutError(TimeoutError):
    pass


class TaskCancelledError(Exception):
    pass


class MinerNetworkError(Exception):
    pass


class MinerDataError(Exception):
    pass


class MinerModelError(Exception):
    pass


def check_ollama():
    try:
        models = ollama.list()
        model_names = [m['model'] for m in models.get('models', [])]
        global INSTALLED_OLLAMA_MODELS
        INSTALLED_OLLAMA_MODELS = set(model_names)
        required = MODEL_SET['cod']
        matched = any(required in name for name in model_names)
        if not matched:
            log(f"ERROR model {required} not found. Run: ollama pull {required}")
            sys.exit(1)
        log(f"OK Ollama is ready, model {required} is available.")
        if INSTALLED_OLLAMA_MODELS:
            preview = ", ".join(sorted(INSTALLED_OLLAMA_MODELS)[:8])
            log(f"Installed Ollama models: {preview}")
    except Exception:
        log("ERROR Ollama is not running. Start Ollama first.")
        sys.exit(1)


def refresh_installed_ollama_models():
    global INSTALLED_OLLAMA_MODELS
    try:
        models = ollama.list()
        model_names = [m["model"] for m in models.get("models", []) if m.get("model")]
        INSTALLED_OLLAMA_MODELS = set(model_names)
        return sorted(INSTALLED_OLLAMA_MODELS)
    except Exception:
        return sorted(INSTALLED_OLLAMA_MODELS)


def normalize_model_name(model_name: str) -> str:
    return (model_name or "").strip()


def get_requested_task_model(task: dict) -> str:
    direct_model = normalize_model_name(task.get("model"))
    if direct_model:
        return direct_model
    context = task.get("context")
    if isinstance(context, dict):
        return normalize_model_name(context.get("model"))
    return ""


def is_model_installed(model_name: str) -> bool:
    model_name = normalize_model_name(model_name)
    if not model_name:
        return False
    return model_name in INSTALLED_OLLAMA_MODELS


def is_model_allowed(model_name: str) -> bool:
    model_name = normalize_model_name(model_name)
    if not model_name:
        return False
    if model_name == MODEL_SET["cod"]:
        return True
    if model_name in EXTRA_ALLOWED_MODELS:
        return True
    if ALLOW_ANY_INSTALLED_MODEL and is_model_installed(model_name):
        return True
    return False


def resolve_task_model(task: dict) -> str:
    requested_model = get_requested_task_model(task)
    if not requested_model:
        return MODEL_SET["cod"]
    if not is_model_installed(requested_model):
        raise MinerModelError(f"requested model not installed: {requested_model}")
    if not is_model_allowed(requested_model):
        raise MinerModelError(f"requested model not allowed: {requested_model}")
    return requested_model


def url_to_base64(url: str) -> str:
    if url.startswith("data:image/"):
        try:
            header, encoded = url.split(",", 1)
        except ValueError:
            raise Exception("invalid data url")
        image_bytes = base64.b64decode(encoded)
        if len(image_bytes) > IMAGE_MAX_BYTES:
            raise Exception("image too large")
        return base64.b64encode(image_bytes).decode('utf-8')
    if url.startswith("http://") or url.startswith("https://"):
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Cache-Control": "no-cache"
        }
        with requests.get(url, timeout=(5, 10), headers=headers, stream=True) as response:
            response.raise_for_status()
            content_length = int(response.headers.get("Content-Length", 0) or 0)
            if content_length > IMAGE_MAX_BYTES:
                raise Exception("image too large")

            image_bytes = bytearray()
            for chunk in response.iter_content(chunk_size=8192):
                if not chunk:
                    continue
                image_bytes.extend(chunk)
                if len(image_bytes) > IMAGE_MAX_BYTES:
                    raise Exception("image too large")
        return base64.b64encode(bytes(image_bytes)).decode('utf-8')
    else:
        clean_path = url.strip('"').strip("'")
        with open(clean_path, "rb") as f:
            image_bytes = f.read()
            if len(image_bytes) > IMAGE_MAX_BYTES:
                raise Exception("image too large")
            return base64.b64encode(image_bytes).decode('utf-8')


def gateway_request(endpoint, payload):
    url = f"{GATEWAY_URL.rstrip('/')}/{endpoint.lstrip('/')}"
    full_payload = {**payload, **{
        "miner_name": MINER_NAME,
        "access_token": ACCESS_TOKEN,
        "hwid": HARDWARE_ID,
        "gpu_count": GPU_COUNT,
        "vram_gb": VRAM_GB,
        "installed_models": sorted(INSTALLED_OLLAMA_MODELS),
    }}

    for attempt in range(3):
        try:
            response = GATEWAY_SESSION.post(
                url,
                json=full_payload,
                timeout=(GATEWAY_CONNECT_TIMEOUT, GATEWAY_READ_TIMEOUT)
            )
            if response.status_code == 403:
                try:
                    detail = response.json().get('detail', '')
                except ValueError:
                    detail = response.text
                if "DEVICE_LOCKED" in detail:
                    log(f"婵犵數濮烽。钘壩ｉ崨鏉戠；闁规崘娉涚欢銈吤归悩宸剰闁汇値鍠楅妵鍕冀椤愵澀娌梺?缂傚倸鍊搁崐鎼佸磹閻戣姤鍤勯柛顐ｆ礃閹偤骞栧ǎ顒€濡奸柣顓燁殜楠炴牕菐椤掆偓婵¤偐绱掗埀顒勫磼濞戞氨顔曢梺鐟邦嚟閸婃垵顫濈捄鍝勫殤闁瑰吋鐣崝宥夋偂韫囨稓鍙撻柛銉戝苯鍓伴梺閫炲苯澧柣妤冨█楠炲啴鏁撻悩鍙傘劑鏌曟径濠傛殭缂傚秴锕顐﹀箛閺夊灝绐涘銈嗘濡嫰寮搁幋锔解拻? 闂傚倸鍊搁崐宄懊归崶褏鏆﹂柛顭戝亝閸欏繘鏌ｉ姀銏╃劸缂佲偓婢跺本鍠愰煫鍥ㄦ礀閸ㄦ繂鈹戦悩瀹犲缂佺媴缍侀弻銊モ攽閸℃娈ㄥ┑顔款潐椤ㄥ﹤顫忓ú顏勭闁肩⒈鍓欑敮銉╂⒑闂堚晝绉甸柛锝忕到閻ｇ兘顢涢悙鎻掔獩闁诲孩绋掗…鍥储閸撗呯＜闁绘劦鍓氱欢鑼偓瑙勬处閸撴岸寮查懜闈涱嚤閻庢稒顭囬崢鐢告⒑閸濆嫷鍎涢柛瀣閹便劑宕掗悙瀵稿幍濡炪倖姊婚悺鏂库枔濮椻偓閺岀喖宕ｆ径瀣攭閻庤娲滈崰鏍€侀弮鍫濈妞ゎ厽鍨垫俊鐑芥⒒?HWID {HARDWARE_ID[:8]}...")
                    sys.exit(1)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            log(f"闂傚倸鍊搁崐鎼佸磹妞嬪海鐭嗗〒姘ｅ亾闁诡喖娼″畷鎯邦槷闁哄鐗犻弻锟犲炊閳轰焦鐎婚梺鎼炲妽濡啴骞冨Δ鍛棃婵炴垶鐟﹂崰鎰磽?缂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮闁汇値鍠楅妵鍕冀椤愵澀绮堕梺鎼炲妼閸婂潡寮婚敐澶婎潊闁靛繆鏅濋崝鎼佹⒑濞茶骞楁い銊ョ墦楠炲顫㈠畝鈧悿鈧┑鐐村灦椤洭顢欓崱娑欌拺缂備焦锕╅悞鐣岀磼椤曞懎鐏﹂柟顕嗙節婵＄兘鍩￠崒姘ｅ亾閻㈠憡鐓ユ繝闈涙椤庢顭胯閸ㄥ爼寮诲☉銏犲嵆闁靛繈鍨婚弳顐︽⒑鐠団€虫灍妞ゃ劌鎳橀崺銏ゅ箻鐠囨彃鐎銈嗘⒒閺咁偅绂嶉鍫熲拻濞达絼璀﹂悞楣冩煥閺囨ê鍔︽鐐插暣瀹曟帡鎮欓懠顒傛毇?(闂傚倸鍊搁崐宄懊归崶顒夋晪鐟滃繘骞戦姀銈呯疀妞ゆ棁妫勬惔濠囨⒑閻熼偊鍤熷┑顕€娼ч悾鐑藉蓟閵夛妇鍘遍梺鏂ユ櫅閸熶即鍩婇弴銏＄厱?{attempt + 1}/3): {e}")
            time.sleep(5)
    return None


def truncate_result_delta(result_delta: str) -> str:
    if len(result_delta) > STREAM_RESULT_DELTA_MAX_LEN:
        return result_delta[-STREAM_RESULT_DELTA_MAX_LEN:]
    return result_delta


def _execute_task_stream_update(task_id: str, result_text: str, status: str, result_delta: str = "", first_token_ms=None):
    if not supabase:
        return
    payload = {'status': status}
    if status != 'processing':
        payload['result'] = result_text
        if not result_delta:
            payload['result_delta'] = ""
    if result_delta:
        payload['result_delta'] = truncate_result_delta(result_delta)
    if first_token_ms is not None:
        try:
            task_row = supabase.table('tasks').select('context').eq('id', task_id).limit(1).execute()
            task_context = {}
            if task_row.data:
                raw_context = task_row.data[0].get('context')
                if isinstance(raw_context, dict):
                    task_context = dict(raw_context)
            metrics = task_context.get("metrics") if isinstance(task_context.get("metrics"), dict) else {}
            if not metrics.get("first_token_ms"):
                metrics["first_token_ms"] = float(first_token_ms)
                task_context["metrics"] = metrics
                payload["context"] = task_context
        except Exception as e:
            log(f"WARN failed to persist first_token_ms for task {task_id[:8]}: {e}")
    try:
        supabase.table('tasks').update(payload).eq('id', task_id).execute()
    except Exception as e:
        if 'result_delta' in payload:
            fallback_payload = dict(payload)
            fallback_payload.pop('result_delta', None)
            try:
                supabase.table('tasks').update(fallback_payload).eq('id', task_id).execute()
                return
            except Exception as fallback_error:
                log(f"闂傚倸鍊搁崐鎼佸磹妞嬪海鐭嗗〒姘ｅ亾闁诡喖娼″畷鎯邦槷闁哄鐗犻弻锟犲炊閳轰焦鐎婚梺鎼炲妽濡啴骞冨Δ鍛棃婵炴垶鐟﹂崰鎰磽?濠电姷鏁告慨鐑藉极閹间礁纾绘繛鎴欏焺閺佸銇勯幘璺烘瀾闁告瑥绻愯灃闁挎繂鎳庨弸銈夋煛娴ｅ壊鍎戦柟鎻掓啞閹棃濡搁妷褏鏉介梻渚€娼ц墝闁哄懏绮撳畷鎴﹀礋椤栨稓鍘遍棅顐㈡处濞叉牜鏁捄濂界懓顭ㄩ崘顏喰ㄥ┑顔硷攻濡炰粙骞冮埄鍐╁劅闁靛繆鈧櫕鐎惧┑锛勫亼閸娧呭緤閼测晛鍨濇繛鍡楃箳閺嗭箓鏌熸潏鍓х暠缂佺姴顭烽幃妤€鈽夊▍顓т邯椤㈡捇骞樼紒妯煎幗闂佺粯鏌ㄩ幗婊堝箠閸愵喗鍊垫慨妯煎帶濞呭秶鈧娲橀悷锔剧矉閹烘柡鍋撻敐搴′簮闁归攱妞藉娲川婵犲嫮鐣甸柣搴㈠嚬閸撶喖宕洪埀顒併亜閹烘垵鈧綊宕甸埀顒€顪冮妶搴″箹闁诲繑绻堥敐鐐差煥閸繄鍔﹀銈嗗笂濡炴帞鎹㈤崱娑欑厽闁靛繈鍩勯悞鍓х磼閻欏懐绉柡宀嬬到铻ｉ柛婵嗗濮ｆ劕顪? {fallback_error}")
                return
        log(f"闂傚倸鍊搁崐鎼佸磹妞嬪海鐭嗗〒姘ｅ亾闁诡喖娼″畷鎯邦槷闁哄鐗犻弻锟犲炊閳轰焦鐎婚梺鎼炲妽濡啴骞冨Δ鍛棃婵炴垶鐟﹂崰鎰磽?濠电姷鏁告慨鐑藉极閹间礁纾绘繛鎴欏焺閺佸銇勯幘璺烘瀾闁告瑥绻愯灃闁挎繂鎳庨弸銈夋煛娴ｅ壊鍎戦柟鎻掓啞閹棃濡搁妷褏鏉介梻渚€娼ц墝闁哄懏绮撳畷鎴﹀礋椤栨稓鍘遍棅顐㈡处濞叉牜鏁捄濂界懓顭ㄩ崘顏喰ㄥ┑顔硷攻濡炰粙骞冮埄鍐╁劅闁靛繆鈧櫕鐎惧┑锛勫亼閸娧呭緤閼测晛鍨濇繛鍡楃箳閺嗭箓鏌熸潏鍓х暠缂佺姴顭烽幃妤€鈽夊▍顓т邯椤㈡捇骞樼紒妯煎幗闂佺粯鏌ㄩ幗婊堝箠閸愵喗鍊垫慨妯煎帶濞呭秶鈧娲橀悷锔剧矉閹烘柡鍋撻敐搴′簮闁归攱妞藉娲川婵犲嫮鐣甸柣搴㈠嚬閸撶喖宕洪埀顒併亜閹烘垵鈧綊宕甸埀顒€顪冮妶搴″箹闁诲繑绻堥敐鐐差煥閸繄鍔﹀銈嗗笂濡炴帞鎹㈤崱娑欑厽闁靛繈鍩勯悞鍓х磼閻欏懐绉柡宀嬬到铻ｉ柛婵嗗濮ｆ劕顪? {e}")


def _stream_update_loop():
    while True:
        task_id, result_text, status, result_delta, first_token_ms = stream_update_queue.get()
        try:
            _execute_task_stream_update(task_id, result_text, status, result_delta, first_token_ms=first_token_ms)
        finally:
            stream_update_queue.task_done()


def start_stream_update_worker():
    global stream_update_worker
    if stream_update_worker or not supabase:
        return
    stream_update_worker = threading.Thread(
        target=_stream_update_loop,
        name="supabase-stream-writer",
        daemon=True
    )
    stream_update_worker.start()


def update_task_stream(task_id: str, result_text: str, status: str, result_delta: str = "", sync: bool = False, first_token_ms=None):
    if not supabase:
        return
    result_delta = truncate_result_delta(result_delta)
    if sync:
        _execute_task_stream_update(task_id, result_text, status, result_delta, first_token_ms=first_token_ms)
        return
    try:
        stream_update_queue.put_nowait((task_id, result_text, status, result_delta, first_token_ms))
    except queue.Full:
        log(f"WARN stream update queue full, skipped one delta update for task {task_id[:8]}")


def get_task_runtime_status(task_id: str) -> str:
    if not supabase:
        return ""
    try:
        res = supabase.table("tasks").select("status").eq("id", task_id).limit(1).execute()
        if not res.data:
            return ""
        return str(res.data[0].get("status") or "").lower()
    except Exception:
        return ""


def raise_if_task_cancelled(task_id: str, last_checked_at: float, interval: float = 0.35) -> float:
    now = time.time()
    if now - last_checked_at < interval:
        return last_checked_at
    status = get_task_runtime_status(task_id)
    if status == "cancelled":
        raise TaskCancelledError("task cancelled by user")
    return now


def build_local_profile_payload():
    installed_models = refresh_installed_ollama_models()
    return {
        "status": "success",
        "profile": {
            "miner_name": MINER_NAME,
            "hwid": HARDWARE_ID,
            "vram_gb": VRAM_GB,
            "gpu_count": GPU_COUNT,
            "tier": HARDWARE_LEVEL,
            "model_limit": MODEL_SET["cod"],
            "capability_score": LOCAL_PROFILE_CAPABILITY["score"],
            "capability_label": LOCAL_PROFILE_CAPABILITY["label"],
            "accepted_sources": ACCEPTED_SOURCES,
            "excluded_sources": EXCLUDED_SOURCES,
            "installed_models": installed_models,
            "auto_claim_enabled": is_auto_claim_enabled(),
        },
    }


class LocalProfileHandler(BaseHTTPRequestHandler):
    def _send_json(self, payload, status_code=200):
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.end_headers()

    def do_GET(self):
        if self.path.rstrip("/") != "/miner-profile":
            self._send_json({"status": "error", "message": "Not found"}, status_code=404)
            return
        self._send_json(build_local_profile_payload())

    def do_POST(self):
        if self.path.rstrip("/") != "/miner-control":
            self._send_json({"status": "error", "message": "Not found"}, status_code=404)
            return
        try:
            content_len = int(self.headers.get("Content-Length", "0") or "0")
            raw = self.rfile.read(content_len) if content_len > 0 else b"{}"
            payload = json.loads(raw.decode("utf-8") or "{}")
        except Exception:
            self._send_json({"status": "error", "message": "Invalid JSON"}, status_code=400)
            return

        enabled = payload.get("auto_claim_enabled")
        if isinstance(enabled, str):
            enabled = enabled.strip().lower() in ("1", "true", "yes", "on")
        else:
            enabled = bool(enabled)
        set_auto_claim_enabled(enabled)
        self._send_json({
            "status": "success",
            "auto_claim_enabled": is_auto_claim_enabled(),
            "miner_name": MINER_NAME,
        })

    def log_message(self, format, *args):
        return


def start_local_profile_server():
    def serve():
        try:
            server = ThreadingHTTPServer((LOCAL_PROFILE_HOST, LOCAL_PROFILE_PORT), LocalProfileHandler)
            log(f"Local miner profile endpoint online at http://{LOCAL_PROFILE_HOST}:{LOCAL_PROFILE_PORT}/miner-profile")
            server.serve_forever()
        except Exception as e:
            log(f"WARN local miner profile endpoint failed: {e}")

    threading.Thread(target=serve, name="local-profile-http", daemon=True).start()


def run_inference_with_retry(task: dict, task_id: str, retries: int = INFERENCE_RETRIES):
    for attempt in range(retries + 1):
        try:
            result_text, token_count, first_token_ms = run_inference_stream(task, task_id, attempt=attempt)
            if not result_text or not result_text.strip():
                if bool(task.get("deep_think", False)):
                    raise MinerModelError("empty model response")
                result_text = build_last_resort_standard_answer(task, "")
                token_count = max(token_count, len(result_text))
            if not bool(task.get("deep_think", False)):
                normalized = normalize_model_output(result_text, False)
                if not is_valid_standard_answer(normalized):
                    language_hint = detect_language_hint(str(task.get("prompt") or ""))
                    repaired_text, repaired_tokens = repair_standard_answer(
                        resolve_task_model(task),
                        task,
                        result_text,
                        SYSTEM_PROMPT,
                        build_ollama_options(task, attempt=attempt),
                        language_hint=language_hint,
                    )
                    normalized = normalize_model_output(repaired_text, False)
                    if not is_valid_standard_answer(normalized):
                        normalized = build_last_resort_standard_answer(task, result_text)
                    result_text = normalized
                    token_count = max(token_count, repaired_tokens)
                else:
                    result_text = normalized
            return result_text, token_count, first_token_ms
        except TaskCancelledError:
            raise
        except TimeoutError:
            raise
        except Exception as e:
            if attempt >= retries:
                if not bool(task.get("deep_think", False)):
                    fallback = build_last_resort_standard_answer(task, str(e))
                    return fallback, max(1, len(fallback)), None
                raise
            log(f"闂傚倸鍊搁崐鎼佸磹妞嬪海鐭嗗〒姘ｅ亾闁诡喖娼″畷鎯邦槷闁哄鐗犻弻锟犲炊閳轰焦鐎婚梺鎼炲妽濡啴骞冨Δ鍛棃婵炴垶鐟﹂崰鎰磽?闂傚倸鍊搁崐鎼佸磹瀹勬噴褰掑炊椤掑﹦绋忔繝銏ｆ硾椤戝洭銆呴幓鎹楀綊鎮╁顔煎壈缂備讲鍋撳鑸靛姇缁犺绻涢敐搴″濠德ゅ亹缁辨帡鎮╁畷鍥р吂闂佸疇顫夐崹鍧楀箖閳哄懎绠甸柟鐑樻尰椤斿嫮绱撻崒娆戭槮濠⒀冮叄瀹曟垿濡堕崪浣告闂佸壊鍋呭ú鏍煁閸ャ劊浜滈柟鏉跨仛缁舵岸鏌涢幋婊呯煓闁诡喛娉涢～婵嬵敇閻樼數鏉芥繝娈垮枛閿曘儱顪冩禒瀣祦闁归偊鍘介崕鐔兼煥濠靛棗鈧綊锝炲澶嬧拻濞达絿顭堥幃鎴炰繆閻愬弶鍋ョ€规洖婀遍幑鍕惞鐟欏嫭顔曢梻浣烘嚀婢у酣鎮洪弮鍫濇瀬鐎广儱妫涚粻楣冩煙鐎电鍓遍柣鎺旀櫕缁辨帡骞囬闂存濠殿喖锕ュ浠嬬嵁閺嶎厽鍊烽柟缁樺笒鑲栭梻鍌欑閹诧繝骞愭繝姘仭闁靛鏅涢悡婵堚偓骞垮劚椤︻垶锝為崨瀛樼厪闁割偅绻冮ˉ婊兠?({attempt + 1}/{retries + 1}): {e}")
            time.sleep(2)


def build_prompt_with_history(prompt: str, context) -> str:
    if not isinstance(context, dict):
        return prompt

    history = context.get("history")
    if not isinstance(history, list):
        return prompt

    normalized_history = []
    for item in history:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role", "")).strip().lower()
        if role not in ("user", "assistant"):
            continue
        content = sanitize_history_message(role, item.get("content", ""))
        if not content:
            continue
        normalized_history.append((role, content))

    if not normalized_history:
        return prompt

    prompt_reserve = min(max(400, len(prompt) + 80), max(400, PROMPT_CHAR_LIMIT - 200))
    history_budget = max(200, PROMPT_CHAR_LIMIT - prompt_reserve)
    recent_budget = max(160, int(history_budget * 0.7))
    summary_budget = max(120, history_budget - recent_budget)
    recent_lines = []
    recent_consumed = 0
    recent_start_index = len(normalized_history)

    for index in range(len(normalized_history) - 1, -1, -1):
        role, content = normalized_history[index]
        prefix = "User" if role == "user" else "Assistant"
        cleaned = content.replace("\r\n", "\n").strip()
        if len(cleaned) > 360:
            cleaned = cleaned[:360] + "..."
        line = f"{prefix}: {cleaned}"
        line_cost = len(line) + 1
        if recent_lines and recent_consumed + line_cost > recent_budget:
            break
        if not recent_lines and line_cost > recent_budget:
            line = f"{prefix}: {cleaned[:max(80, recent_budget - len(prefix) - 6)]}..."
            line_cost = len(line) + 1
        recent_lines.append(line)
        recent_consumed += line_cost
        recent_start_index = index

    summary_lines = []
    summary_consumed = 0
    for role, content in normalized_history[:recent_start_index]:
        prefix = "User asked" if role == "user" else "Assistant replied"
        cleaned = " ".join(content.replace("\r\n", "\n").split())
        if len(cleaned) > 140:
            cleaned = cleaned[:140] + "..."
        line = f"- {prefix}: {cleaned}"
        line_cost = len(line) + 1
        if summary_consumed + line_cost > summary_budget:
            remaining = summary_budget - summary_consumed
            if remaining > 40:
                summary_lines.append(line[:remaining - 3] + "...")
            break
        summary_lines.append(line)
        summary_consumed += line_cost

    sections = []
    if summary_lines:
        sections.append("Earlier conversation summary:\n" + "\n".join(summary_lines))
    if recent_lines:
        sections.append("Recent conversation:\n" + "\n".join(reversed(recent_lines)))

    if not sections:
        return prompt

    return (
        "\n\n".join(sections) +
        "\n\nCurrent user request:\n" +
        prompt
    )



def detect_language_hint(text: str) -> str:
    raw = str(text or "")
    if re.search(r"[\u4e00-\u9fff]", raw):
        return "zh"
    return "en"

CODE_TASK_KEYWORDS = (
    "code", "coding", "programming", "function", "class", "algorithm", "python", "java", "javascript",
    "typescript", "golang", "go ", "rust", "c++", "c#", "bug", "error", "fix",
    "refactor", "implement", "pseudocode", "sql", "interface", "api", "script", "leetcode", "scheduler",
    "concurrency", "thread", "cache", "data structure"
)


def is_code_task(task: dict) -> bool:
    if not CODE_WORKFLOW_ENABLED:
        return False
    if task.get("image_url"):
        return False

    prompt = str(task.get("prompt") or "").lower()
    context = task.get("context") if isinstance(task.get("context"), dict) else {}
    model = str(task.get("model") or context.get("model") or "").lower()

    if "coder" in model:
        return True
    if "```" in prompt:
        return True
    if re.search(r"\b(def |class |function |interface |select |insert |update |delete )", prompt):
        return True

    return any(keyword in prompt for keyword in CODE_TASK_KEYWORDS)


def trim_stage_text(text: str, limit: int = CODE_MAX_STAGE_CHARS) -> str:
    cleaned = (text or "").strip()
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[:limit].rstrip() + "\n...(truncated)"


def build_code_history_context(task: dict) -> str:
    context = task.get("context") if isinstance(task.get("context"), dict) else {}
    prompt = str(task.get("prompt") or "")
    history_prompt = build_prompt_with_history(prompt, context)
    if len(history_prompt) <= CODE_HISTORY_CHARS:
        return history_prompt
    return history_prompt[-CODE_HISTORY_CHARS:]


def build_code_stage_prompt(task_prompt: str, stage_name: str, requirements: str, prior_sections: list[tuple[str, str]]) -> str:
    sections = [f"Original task:\n{task_prompt}"]
    if prior_sections:
        sections.append(
            "Prior work:\n" +
            "\n\n".join(
                f"{title}:\n{trim_stage_text(content)}"
                for title, content in prior_sections if content
            )
        )
    sections.append(f"Current stage: {stage_name}\n{requirements}")
    return "\n\n".join(sections)


def is_code_eval_task(task: dict) -> bool:
    context = task.get("context") if isinstance(task.get("context"), dict) else {}
    source = str(context.get("source") or "").strip().lower()
    output_mode = str(context.get("output_mode") or "").strip().lower()
    prompt = str(task.get("prompt") or "")
    if source == "code_eval" or output_mode == "code_only":
        return True
    if "return runnable python code in a fenced code block" in prompt.lower():
        return True
    return False


def extract_required_python_symbols(task_prompt: str) -> list[str]:
    patterns = [
        r"defines\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(",
        r"defines\s+([A-Za-z_][A-Za-z0-9_]*)\b",
        r"for\s+an?\s+([A-Za-z_][A-Za-z0-9_]*)\s+class",
        r"for\s+a\s+([A-Za-z_][A-Za-z0-9_]*)\s+class",
        r"class\s+([A-Za-z_][A-Za-z0-9_]*)\b",
    ]
    found = []
    for pattern in patterns:
        for match in re.findall(pattern, task_prompt, flags=re.IGNORECASE):
            if match not in found:
                found.append(match)
    return found


def build_required_symbol_guidance(symbols: list[str]) -> str:
    if not symbols:
        return (
            "The final code must directly answer the task, avoid undefined helper names, "
            "and keep the public API aligned with the prompt."
        )
    joined = ", ".join(symbols)
    return (
        f"You must define these required public symbols exactly as named: {joined}. "
        "Do not rename them, wrap them in another class, or replace them with different APIs. "
        "Do not reference undefined helper names."
    )


def extract_required_method_names(task_prompt: str) -> list[str]:
    method_names = []
    for match in re.findall(r"([A-Za-z_][A-Za-z0-9_]*)\s*\(", task_prompt):
        lowered = match.lower()
        if lowered in {"select", "from", "where", "and", "or", "limit", "order"}:
            continue
        if match not in method_names:
            method_names.append(match)
    return method_names


def build_interface_checklist(symbols: list[str], methods: list[str]) -> str:
    checklist = []
    if symbols:
        checklist.append("Required symbols: " + ", ".join(symbols))
    if methods:
        checklist.append("Referenced callables/methods: " + ", ".join(methods))
    if not checklist:
        checklist.append("No explicit symbol list extracted; keep the public API aligned with the task wording.")
    checklist.append("Do not invent helper types or rename the requested API.")
    checklist.append("If a class is requested, define the class at top level and implement its expected methods.")
    checklist.append("If a function is requested, define that exact function name at top level.")
    return " | ".join(checklist)


def extract_last_fenced_code_block(text: str) -> str:
    if not text:
        return ""
    matches = re.findall(r"```(?:[A-Za-z0-9_+-]+)?\s*(.*?)```", text, flags=re.DOTALL)
    if matches:
        return matches[-1].strip()
    return ""


def normalize_code_only_output(text: str) -> str:
    code_block = extract_last_fenced_code_block(text)
    if code_block:
        return f"```python\n{code_block}\n```"
    cleaned = (text or "").strip()
    if not cleaned:
        return cleaned
    lines = []
    for raw_line in cleaned.splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped:
            lines.append("")
            continue
        lower = stripped.lower()
        if lower.startswith("thinking"):
            continue
        if stripped in ("Known information:", "Uncertainties:", "Analysis:", "Conclusion:"):
            continue
        if re.match(r"^\d+\.\s+\*\*", stripped):
            continue
        if stripped.startswith("* ") or stripped.startswith("- "):
            continue
        lines.append(line)
    normalized = "\n".join(lines).strip()
    if not normalized:
        return cleaned
    return f"```python\n{normalized}\n```"


def generate_with_workflow(model: str, prompt: str, system_prompt: str, options: dict, *, images=None, think_mode: bool = False) -> tuple[str, int]:
    return ollama_generate_with_continuation(
        model,
        prompt,
        images,
        think_mode,
        system_prompt,
        options,
        language_hint=detect_language_hint(prompt),
    )


def build_code_workflow_options(task: dict, attempt: int = 0) -> dict:
    options = dict(build_ollama_options(task, attempt=attempt))
    options["num_predict"] = max(options.get("num_predict", 256), 512 if LOW_VRAM_MODE else 1024)
    options["num_ctx"] = max(options.get("num_ctx", 768), 1024 if LOW_VRAM_MODE else 2048)
    return options


def run_code_workflow(task: dict, model: str, system_prompt: str, attempt: int = 0) -> tuple[str, int]:
    task_prompt = build_code_history_context(task)
    options = build_code_workflow_options(task, attempt=attempt)
    total_tokens = 0
    stages: list[tuple[str, str]] = []
    code_only_mode = is_code_eval_task(task)
    required_symbols = extract_required_python_symbols(task_prompt)
    required_methods = extract_required_method_names(task_prompt)
    required_symbol_guidance = build_required_symbol_guidance(required_symbols)
    interface_checklist = build_interface_checklist(required_symbols, required_methods)

    architecture_prompt = build_code_stage_prompt(
        task_prompt,
        "Step 1 - module plan",
        "List the modules or implementation parts needed. Do not write code. Be concrete and concise.",
        []
    )
    architecture, tokens = generate_with_workflow(model, architecture_prompt, system_prompt, options)
    total_tokens += tokens
    stages.append(("Module plan", architecture))

    data_prompt = build_code_stage_prompt(
        task_prompt,
        "Step 2 - data structures",
        "Design the core data structures, state, function signatures, and interfaces. Do not write full code yet.",
        stages
    )
    data_structures, tokens = generate_with_workflow(model, data_prompt, system_prompt, options)
    total_tokens += tokens
    stages.append(("Data structures", data_structures))

    flow_prompt = build_code_stage_prompt(
        task_prompt,
        "Step 3 - execution flow",
        "Write pseudocode or a precise execution flow for the solution. Keep it implementation-oriented.",
        stages
    )
    execution_flow, tokens = generate_with_workflow(model, flow_prompt, system_prompt, options)
    total_tokens += tokens
    stages.append(("Execution flow", execution_flow))

    edge_prompt = build_code_stage_prompt(
        task_prompt,
        "Step 4 - edge cases",
        "Identify dependencies, failure modes, concurrency issues, invalid input handling, and other edge cases.",
        stages
    )
    edge_cases, tokens = generate_with_workflow(model, edge_prompt, system_prompt, options)
    total_tokens += tokens
    stages.append(("Edge cases", edge_cases))

    draft_prompt = build_code_stage_prompt(
        task_prompt,
        "Step 5 - draft implementation",
        (
            "Write the complete code. Prefer one self-contained solution. "
            f"{required_symbol_guidance} "
            "If the task implies example inputs or expected outputs, make the implementation consistent with them. "
            "If the user did not request explanation, keep commentary minimal."
        ),
        stages
    )
    draft_code, tokens = generate_with_workflow(model, draft_prompt, system_prompt, options)
    total_tokens += tokens
    stages.append(("Draft code", draft_code))

    review_prompt = build_code_stage_prompt(
        task_prompt,
        "Step 6 - self review",
        (
            f"Review the draft implementation and list at least {CODE_REVIEW_MIN_ISSUES} concrete issues, bugs, edge-case gaps, "
            "or maintainability risks. For each issue, explain the impact and the fix. If the code is already solid, still identify "
            "the most likely failure points and how to harden them. "
            f"{required_symbol_guidance} "
            "Explicitly check for: missing required function/class names, undefined variables or helper names, "
            "wrong return order, and mismatch with examples or constraints mentioned in the task."
        ),
        stages
    )
    review_notes, tokens = generate_with_workflow(model, review_prompt, system_prompt, options)
    total_tokens += tokens
    stages.append(("Self review", review_notes))

    interface_prompt = build_code_stage_prompt(
        task_prompt,
        "Step 6.5 - interface checklist",
        (
            "Produce a short interface checklist before finalizing code. "
            f"{interface_checklist} "
            "State whether each required symbol and method is present, whether any undefined helper name remains, "
            "and whether the final public API exactly matches the task."
        ),
        stages
    )
    interface_check, tokens = generate_with_workflow(model, interface_prompt, system_prompt, options)
    total_tokens += tokens
    stages.append(("Interface checklist", interface_check))

    final_prompt = build_code_stage_prompt(
        task_prompt,
        "Step 7 - final repaired implementation",
        (
            "Produce the final repaired code after applying the review fixes. "
            + (
                "Return only one fenced Python code block. "
                "Do not output analysis, headings, bullet points, thinking text, or any text before or after the code block. "
                f"{required_symbol_guidance} "
                f"{interface_checklist} "
                "Before finalizing, mentally verify that the required symbols exist and that the code does not reference undefined names. "
                "If the task includes example behavior, ensure the implementation matches those examples before answering. "
                "The response must be directly executable after extracting the fenced code block."
                if code_only_mode else
                "Return the final answer using this structure in Chinese:\n"
                "\u5df2\u77e5\u4fe1\u606f\uff1a\n- ...\n"
                "\u4e0d\u786e\u5b9a\u6027\uff1a\n- ...\n"
                "\u5206\u6790\uff1a\n- ...\n"
                "\u7ed3\u8bba\uff1a\n```language\n...\n```\n"
                "The \u7ed3\u8bba section must contain the final code."
            )
        ),
        stages
    )
    final_answer, tokens = generate_with_workflow(model, final_prompt, system_prompt, options)
    total_tokens += tokens
    if code_only_mode:
        final_answer = normalize_code_only_output(final_answer)
    return final_answer, total_tokens


def build_inference_payload(task: dict, attempt: int = 0):
    prompt = task.get('prompt') or ""
    image_url = task.get('image_url')
    deep_think = bool(task.get('deep_think', False))
    context = task.get('context')
    images = None
    image_analysis_guard = (
        "Analyze only what is visibly supported by the image. "
        "Separate visible facts from inference. "
        "Do not invent colors, clothing, identity, or details that are not clearly visible. "
        "If the image is stylized, partial, blurry, low-detail, or uncertain, say that explicitly. "
        "If asked to identify a character, object, or scene, you may provide a best-supported conclusion, "
        "but only after listing concrete visible evidence and reasoning from that evidence. "
        "If evidence is insufficient for a firm identification, say so and keep confidence low."
    )
    image_response_format = (
        "Use this exact structure:\n"
        "Known facts:\n"
        "- ...\n"
        "Uncertainties:\n"
        "- ...\n"
        "Analysis:\n"
        "- ...\n"
        "Conclusion:\n"
        "- ...\n"
        "Confidence: high / medium / low\n"
        "Facts and inference must be clearly separated. "
        "Prefer the most likely correct conclusion that is still defensible from the visible evidence."
    )

    if image_url:
        try:
            images = [url_to_base64(image_url)]
        except requests.RequestException as e:
            raise MinerNetworkError(f"image fetch failed: {e}") from e
        except Exception as e:
            raise MinerDataError(f"image processing failed: {e}") from e

    prompt = build_prompt_with_history(prompt, context)
    language_hint = detect_language_hint(prompt)
    if len(prompt) > PROMPT_CHAR_LIMIT:
        prompt = prompt[:PROMPT_CHAR_LIMIT] + "\n\n(Trimmed for stable inference on constrained VRAM.)"
    if LOW_VRAM_MODE and attempt > 0:
        prompt = (
            "Answer concisely with up to 5 bullet points. Avoid long reasoning. "
            "Return only final answer.\n\n" + prompt
        )

    protocol_instruction = build_universal_llm_protocol(deep_think)
    if deep_think and images:
        final_prompt = (
            f"{protocol_instruction}\n\n"
            f"{image_analysis_guard}\n\n"
            f"{image_response_format}\n\n"
            f"Analyze the image and provide a complete answer for: {prompt}"
        )
    elif deep_think:
        final_prompt = (
            f"{protocol_instruction}\n\n"
            f"{prompt}"
        )
    elif images:
        final_prompt = (
            f"{protocol_instruction}\n\n"
            f"{image_analysis_guard} "
            f"{image_response_format} "
            f"Analyze the image and answer: {prompt}"
        )
    else:
        final_prompt = (
            f"{protocol_instruction}\n\n"
            f"{prompt}"
        )

    return final_prompt, images, deep_think, SYSTEM_PROMPT, language_hint

def build_ollama_options(task: dict, attempt: int = 0):
    deep_think = bool(task.get("deep_think", False))
    num_predict = OLLAMA_NUM_PREDICT if deep_think else OLLAMA_NORMAL_NUM_PREDICT
    if LOW_VRAM_MODE and deep_think:
        num_predict = min(num_predict, 320)
    if attempt > 0:
        num_predict = max(128, num_predict // (2 ** attempt))
    num_ctx = OLLAMA_NUM_CTX
    if attempt > 0:
        num_ctx = max(512, num_ctx // (2 ** attempt))
    return {
        "num_predict": num_predict,
        "num_ctx": num_ctx,
        "num_batch": OLLAMA_NUM_BATCH,
    }


def extract_generate_texts(payload) -> tuple[str, str]:
    if payload is None:
        return "", ""
    if isinstance(payload, dict):
        response_text = str(payload.get("response", "") or "")
        content_text = str(payload.get("content", "") or "")
        msg = payload.get("message")
        if isinstance(msg, dict):
            content_text = content_text or str(msg.get("content", "") or "")
        thinking_text = str(payload.get("thinking", "") or "")
    else:
        response_text = str(getattr(payload, "response", "") or "")
        content_text = str(getattr(payload, "content", "") or "")
        msg = getattr(payload, "message", None)
        content_text = content_text or str(getattr(msg, "content", "") or "")
        thinking_text = str(getattr(payload, "thinking", "") or "")
    answer_text = response_text or content_text
    return thinking_text, answer_text


def strip_protocol_tags(text: str) -> str:
    cleaned = str(text or "")
    cleaned = re.sub(r"</?(?:think|answer)>", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip()


def build_universal_llm_protocol(deep_think: bool) -> str:
    if deep_think:
        return (
            "/think\n"
            "You are using DEEP THINK mode.\n"
            "Use the same language as the user's latest message.\n"
            "You must output exactly two tagged sections and nothing else:\n"
            "<think>\n"
            "Write concise but complete reasoning. For simple questions, keep this very short. "
            "For complex questions, reason enough to be useful.\n"
            "</think>\n"
            "<answer>\n"
            "Write the final answer only.\n"
            "</answer>\n"
            "Do not repeat system prompts, prior hidden text, continuation instructions, or meta commentary."
        )
    return (
        "/no_think\n"
        "You are using NORMAL mode.\n"
        "Use the same language as the user's latest message.\n"
        "Do all reasoning silently. Never reveal analysis, chain-of-thought, planning, alternatives, or meta commentary.\n"
        "Do not start with phrases like 'Okay', 'First', 'I need to', 'Let me', or 'Wait'.\n"
        "You must output exactly one tagged section and nothing else:\n"
        "<answer>\n"
        "Write the final answer only.\n"
        "</answer>"
    )


def extract_tag_content(text: str, tag: str) -> str:
    raw = str(text or "")
    match = re.search(rf"<{tag}>(.*?)(?:</{tag}>|$)", raw, flags=re.IGNORECASE | re.DOTALL)
    return match.group(1).strip() if match else ""


def normalize_model_output(text: str, deep_think: bool) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    think = extract_tag_content(raw, "think")
    answer = extract_tag_content(raw, "answer")
    if deep_think:
        if answer or think:
            return f"<think>{think}</think><answer>{answer}</answer>"
        return f"<think></think><answer>{strip_protocol_tags(raw)}</answer>"
    if answer:
        return f"<answer>{answer}</answer>"
    if "</think>" in raw.lower():
        tail = re.split(r"</think>", raw, flags=re.IGNORECASE)[-1]
        return f"<answer>{strip_protocol_tags(tail)}</answer>"
    cleaned = re.sub(r"<think>.*?(?:</think>|$)", "", raw, flags=re.IGNORECASE | re.DOTALL)
    return f"<answer>{strip_protocol_tags(cleaned)}</answer>"


def looks_like_reasoning_leak(text: str) -> bool:
    sample = str(text or "").strip()[:320].lower()
    if not sample:
        return False
    return bool(
        re.match(
            r"^(okay|ok,|first,|let me|i need to|i should|we need to|hmm|wait,|the user|user asked|thinking process|analysis:|current user request|continue in the same language as before|the previous responses|the assistant has been replying)",
            sample,
        )
    )


def extract_fallback_answer_from_leak(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    answer = extract_tag_content(raw, "answer")
    if answer:
        return answer
    lines = [
        line.strip()
        for line in re.split(r"\n+", raw)
        if line and line.strip()
    ]
    lines = [
        line for line in lines
        if not looks_like_reasoning_leak(line)
        and not re.match(r"^(the user|current user|possible response|the key points|the instruction|wait,|but the instruction)", line.strip(), flags=re.IGNORECASE)
    ]
    short_cjk = next((line for line in reversed(lines) if re.search(r"[\u4e00-\u9fff]", line) and len(line) <= 80), "")
    if short_cjk:
        return strip_protocol_tags(short_cjk)
    short_line = next((line for line in reversed(lines) if len(line) <= 120), "")
    return strip_protocol_tags(short_line)


def sanitize_history_message(role: str, content: str) -> str:
    role_value = str(role or "").strip().lower()
    text = strip_protocol_tags(str(content or "").strip())
    if role_value not in ("user", "assistant") or not text:
        return ""
    if role_value == "assistant":
        fallback = extract_fallback_answer_from_leak(text)
        if looks_like_reasoning_leak(text):
            text = fallback
        if not text or looks_like_reasoning_leak(text):
            return ""
    return text


def is_valid_standard_answer(text: str) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return False
    answer = extract_tag_content(raw, "answer") or strip_protocol_tags(raw)
    if not answer.strip():
        return False
    if looks_like_reasoning_leak(answer) and len(answer) > 80:
        return False
    return True


def build_last_resort_standard_answer(task: dict, leaked_text: str = "") -> str:
    fallback = extract_fallback_answer_from_leak(leaked_text)
    if fallback and not looks_like_reasoning_leak(fallback):
        return f"<answer>{fallback}</answer>"

    prompt = str(task.get("prompt") or "").strip()
    lang = detect_language_hint(f"{prompt}\n{leaked_text}")
    prompt_lower = prompt.lower()
    greeting_patterns = [
        r"^\s*你好[呀啊吗]?\s*$",
        r"^\s*嗨\s*$",
        r"^\s*hello\s*$",
        r"^\s*hi\s*$",
    ]
    if any(re.match(pattern, prompt_lower, flags=re.IGNORECASE) for pattern in greeting_patterns):
        if lang == "zh":
            return "<answer>你好！有什么可以帮你的？</answer>"
        return "<answer>Hello! How can I help you?</answer>"

    if lang == "zh":
        return "<answer>我已收到你的问题。请再具体一点，我会直接给出答案。</answer>"
    return "<answer>I received your request. Please provide one more detail and I will answer directly.</answer>"


def repair_standard_answer(model: str, task: dict, leaked_text: str, system_prompt: str, options: dict, language_hint: str = "en") -> tuple[str, int]:
    fallback = extract_fallback_answer_from_leak(leaked_text)
    repair_prompts = [
        (
            "/no_think\n"
            "The draft below is invalid because it contains reasoning instead of the final answer.\n"
            "Rewrite it into the final answer only, in the same language as the user's request.\n"
            "Do not explain. Do not add reasoning. Do not add commentary.\n"
            "Output exactly:\n"
            "<answer>\n"
            "Final answer only.\n"
            "</answer>\n\n"
            f"User request:\n{task.get('prompt') or ''}\n\n"
            f"Invalid draft:\n{str(leaked_text or '')[:2200]}"
        ),
        (
            "/no_think\n"
            "Answer the user's request directly in the same language as the user.\n"
            "Ignore any previous hidden reasoning, continuation instructions, or invalid draft text.\n"
            "Output exactly:\n"
            "<answer>\n"
            "Final answer only.\n"
            "</answer>\n\n"
            f"User request:\n{task.get('prompt') or ''}"
        ),
    ]
    for repair_prompt in repair_prompts:
        repaired, meta = ollama_generate_once(model, repair_prompt, None, False, system_prompt, options)
        normalized = normalize_model_output(repaired, False)
        answer = extract_tag_content(normalized, "answer") or extract_fallback_answer_from_leak(normalized)
        if answer and not looks_like_reasoning_leak(answer):
            return f"<answer>{answer}</answer>", len(answer)
    if fallback:
        return f"<answer>{fallback}</answer>", len(fallback)
    return "<answer>回答格式异常，请重新生成。</answer>", len("回答格式异常，请重新生成。")


def format_generate_text(payload, allow_thinking: bool = False) -> str:
    thinking_text, answer_text = extract_generate_texts(payload)
    raw_answer = str(answer_text or "")
    if allow_thinking and re.search(r"</?(?:think|answer)>", raw_answer, flags=re.IGNORECASE):
        return normalize_model_output(raw_answer, True)
    thinking_text = strip_protocol_tags(thinking_text)
    answer_text = strip_protocol_tags(answer_text)
    if allow_thinking and thinking_text.strip():
        return f"<think>{thinking_text}</think><answer>{answer_text}</answer>"
    return normalize_model_output(answer_text, False)


def extract_generate_meta(payload) -> dict:
    if payload is None:
        return {"done_reason": "", "context": None}
    if isinstance(payload, dict):
        return {
            "done_reason": str(payload.get("done_reason", "") or ""),
            "context": payload.get("context"),
        }
    return {
        "done_reason": str(getattr(payload, "done_reason", "") or ""),
        "context": getattr(payload, "context", None),
    }


def merge_continuation_text(existing: str, new_text: str) -> str:
    if not new_text:
        return existing
    if not existing:
        return new_text
    max_overlap = min(len(existing), len(new_text), 200)
    for overlap in range(max_overlap, 0, -1):
        if existing.endswith(new_text[:overlap]):
            return existing + new_text[overlap:]
    return existing + new_text


def should_continue_generation(text: str, meta: dict, accumulated: str) -> bool:
    if not text or not text.strip():
        return False
    if len(accumulated) >= MAX_OUTPUT_CHARS:
        return False
    reason = str((meta or {}).get("done_reason", "") or "").lower()
    if reason in ("stop", "end_turn"):
        return False
    if reason in ("length", "max_tokens"):
        return True
    return False


def build_continue_prompt(language_hint: str = "en", answer_only: bool = False) -> str:
    if answer_only:
        return (
            "Continue in the same language as before. "
            "Continue only the remaining content inside the current <answer> block. "
            "Do not repeat prior text. "
            "Do not restart the reasoning process. "
            "Do not output new meta-instructions. "
            "Do not open new tags. "
            "Return only the remaining answer content."
        )
    return (
        "Continue in the same language as before. "
        "Continue exactly where you stopped inside the current open tag. "
        "Do not repeat prior text. "
        "Do not restart the answer. "
        "Do not open new tags. "
        "Return only the remaining continuation."
    )

def ollama_generate_worker(model: str, final_prompt: str, images, think_mode: bool, system_prompt: str, options, event_queue):
    try:
        thinking_started = False
        thinking_closed = False
        answer_started = False
        for chunk in ollama.generate(
            model=model,
            prompt=final_prompt,
            system=system_prompt,
            images=images,
            stream=True,
            think=False,
            keep_alive="30m",
            options=options
        ):
            thinking_text, answer_text = extract_generate_texts(chunk)
            token_parts = []
            if think_mode and thinking_text.strip():
                if not thinking_started:
                    token_parts.append("<think>")
                    thinking_started = True
                token_parts.append(thinking_text)
            if answer_text.strip():
                if think_mode and not thinking_text.strip():
                    token_parts.append(answer_text)
                else:
                    if think_mode and thinking_started and not thinking_closed:
                        token_parts.append("</think>")
                        thinking_closed = True
                    if not answer_started:
                        token_parts.append("<answer>")
                        answer_started = True
                    token_parts.append(answer_text)
            event_queue.put({
                "type": "chunk",
                "token": "".join(token_parts) if token_parts else ""
            })
        if think_mode and thinking_started and not thinking_closed:
            event_queue.put({
                "type": "chunk",
                "token": "</think>"
            })
        if answer_started:
            event_queue.put({
                "type": "chunk",
                "token": "</answer>"
            })
        event_queue.put({"type": "done"})
    except Exception as e:
        event_queue.put({
            "type": "error",
            "error": str(e)
        })


def iter_stream_tokens(model: str, final_prompt: str, images, think_mode: bool, system_prompt: str, options):
    thinking_started = False
    thinking_closed = False
    answer_started = False
    for chunk in ollama.generate(
        model=model,
        prompt=final_prompt,
        system=system_prompt,
        images=images,
        stream=True,
        think=False,
        keep_alive="30m",
        options=options
    ):
        thinking_text, answer_text = extract_generate_texts(chunk)
        token_parts = []
        if think_mode and thinking_text.strip():
            if not thinking_started:
                token_parts.append("<think>")
                thinking_started = True
            token_parts.append(thinking_text)
        if answer_text.strip():
            if think_mode and not thinking_text.strip():
                token_parts.append(answer_text)
            else:
                if think_mode and thinking_started and not thinking_closed:
                    token_parts.append("</think>")
                    thinking_closed = True
                if not answer_started:
                    token_parts.append("<answer>")
                    answer_started = True
                token_parts.append(answer_text)
        token = "".join(token_parts)
        if token:
            yield token
    if think_mode and thinking_started and not thinking_closed:
        yield "</think>"
    if answer_started:
        yield "</answer>"


def iter_stream_tokens_http(model: str, final_prompt: str, images, think_mode: bool, system_prompt: str, options):
    payload = {
        "model": model,
        "prompt": final_prompt,
        "stream": True,
        "keep_alive": "30m",
        "options": options,
    }
    if system_prompt:
        payload["system"] = system_prompt
    if images:
        payload["images"] = images

    thinking_started = False
    thinking_closed = False
    answer_started = False

    with requests.post(
        "http://127.0.0.1:11434/api/generate",
        json=payload,
        stream=True,
        timeout=(5, OLLAMA_READ_TIMEOUT_SECONDS),
    ) as response:
        response.raise_for_status()
        for raw_line in response.iter_lines():
            line = raw_line.decode("utf-8", errors="ignore").strip() if isinstance(raw_line, bytes) else str(raw_line or "").strip()
            if not line:
                continue
            try:
                chunk = json.loads(line)
            except json.JSONDecodeError:
                continue
            token = str(chunk.get("response") or "")
            if not token:
                if chunk.get("done"):
                    break
                continue

            if think_mode:
                thinking_text, answer_text = extract_generate_texts({"response": token})
                token_parts = []
                if thinking_text.strip():
                    if not thinking_started:
                        token_parts.append("<think>")
                        thinking_started = True
                    token_parts.append(thinking_text)
                if answer_text.strip():
                    if thinking_started and not thinking_closed:
                        token_parts.append("</think>")
                        thinking_closed = True
                    if not answer_started:
                        token_parts.append("<answer>")
                        answer_started = True
                    token_parts.append(answer_text)
                normalized_token = "".join(token_parts)
                if normalized_token:
                    yield normalized_token
            else:
                if not answer_started:
                    yield "<answer>"
                    answer_started = True
                yield token

            if chunk.get("done"):
                break

    if think_mode and thinking_started and not thinking_closed:
        yield "</think>"
    if answer_started:
        yield "</answer>"


def ollama_generate_once(model: str, final_prompt: str, images, think_mode: bool, system_prompt: str, options, context_state=None):
    kwargs = {
        "model": model,
        "prompt": final_prompt,
        "system": system_prompt,
        "images": images,
        "stream": False,
        "think": False,
        "keep_alive": "30m",
        "options": options,
    }
    if context_state:
        kwargs["context"] = context_state
    result = ollama.generate(
        **kwargs
    )
    return format_generate_text(result, allow_thinking=think_mode), extract_generate_meta(result)


def ollama_generate_with_continuation(model: str, final_prompt: str, images, think_mode: bool, system_prompt: str, options, language_hint: str = "en"):
    accumulated = ""
    token_count = 0
    context_state = None
    current_prompt = final_prompt
    current_images = images
    max_continuations = MAX_CONTINUATIONS if think_mode else MAX_STANDARD_CONTINUATIONS

    for continuation_index in range(max_continuations + 1):
        text, meta = ollama_generate_once(
            model,
            current_prompt,
            current_images,
            think_mode,
            system_prompt,
            options,
            context_state=context_state,
        )
        accumulated = merge_continuation_text(accumulated, text)
        if text:
            token_count += len(text)
        context_state = (meta or {}).get("context") or context_state

        if not think_mode and looks_like_reasoning_leak(text):
            break
        if not should_continue_generation(text, meta, accumulated):
            break

        current_prompt = build_continue_prompt(language_hint, answer_only=think_mode)
        current_images = None
        log(f"INFO continuing generation pass {continuation_index + 1}/{max_continuations} for model {model}")

    return accumulated, token_count


def build_deep_think_stage_inputs(task: dict, attempt: int = 0):
    prompt = task.get('prompt') or ""
    image_url = task.get('image_url')
    context = task.get('context')
    images = None

    if image_url:
        try:
            images = [url_to_base64(image_url)]
        except requests.RequestException as e:
            raise MinerNetworkError(f"image fetch failed: {e}") from e
        except Exception as e:
            raise MinerDataError(f"image processing failed: {e}") from e

    prompt = build_prompt_with_history(prompt, context)
    language_hint = detect_language_hint(prompt)
    if len(prompt) > PROMPT_CHAR_LIMIT:
        prompt = prompt[:PROMPT_CHAR_LIMIT] + "\n\n(Trimmed for stable inference on constrained VRAM.)"
    if LOW_VRAM_MODE and attempt > 0:
        prompt = (
            "Answer concisely with up to 5 bullet points. Avoid long reasoning. "
            "Return only final answer.\n\n" + prompt
        )
    return prompt, images, language_hint


def classify_reasoning_profile(prompt: str) -> str:
    text = str(prompt or "").strip()
    if not text:
        return "simple"
    complex_patterns = [
        r"分析",
        r"评价",
        r"比较",
        r"解释",
        r"为什么",
        r"如何",
        r"方案",
        r"设计",
        r"总结",
        r"推理",
        r"证明",
        r"代码",
        r"原理",
        r"复杂",
        r"analy[sz]e",
        r"compare",
        r"explain",
        r"why\b",
        r"how\b",
        r"design",
        r"plan",
        r"reason",
        r"prove",
        r"implement",
    ]
    if len(text) <= 24 and "\n" not in text and not any(re.search(pattern, text, re.IGNORECASE) for pattern in complex_patterns):
        return "simple"
    return "complex"


def run_deep_think_stream(task: dict, task_id: str, model: str, attempt: int = 0):
    prompt, images, language_hint = build_deep_think_stage_inputs(task, attempt=attempt)
    system_prompt = SYSTEM_PROMPT
    options = build_ollama_options(task, attempt=attempt)
    same_language_instruction = "Use the same language as the user's request."
    reasoning_profile = classify_reasoning_profile(prompt)
    reasoning_guidance = (
        "Keep the reasoning short but complete. Use only the minimum reasoning needed to fully support the answer."
        if reasoning_profile == "simple"
        else "The reasoning may be as detailed as needed, but it must stay focused and complete."
    )
    reasoning_prompt = (
        f"{same_language_instruction} "
        "Think step by step and output only the reasoning process. "
        "Keep it concise, useful, and directly related to solving the user's request. "
        f"{reasoning_guidance} "
        "Do not output any final answer. "
        "Do not use XML or tag wrappers. "
        "Start reasoning immediately.\n\n"
        f"{prompt}"
    )

    full_result = "<think>"
    reasoning_text = ""
    answer_text = ""
    token_count = 0
    first_token_sent = False
    first_token_ms = None
    last_update_time = time.time()
    update_interval = 0.08
    update_min_delta = 4
    inference_start_time = time.time()
    cancel_check_at = 0.0

    for token in iter_stream_tokens(model, reasoning_prompt, images, False, system_prompt, options):
        if time.time() - inference_start_time > MAX_TASK_RUNTIME:
            raise TaskTimeoutError("inference timed out")
        cancel_check_at = raise_if_task_cancelled(task_id, cancel_check_at)
        reasoning_text += token
        full_result = f"<think>{reasoning_text}"
        token_count += len(token)
        if token and first_token_ms is None:
            first_token_ms = max(1, int((time.time() - inference_start_time) * 1000))
        if not first_token_sent or (time.time() - last_update_time > update_interval and len(token) >= 1):
            update_task_stream(task_id, "", 'processing', truncate_result_delta(full_result), sync=True, first_token_ms=first_token_ms if not first_token_sent else None)
            first_token_sent = True
            last_update_time = time.time()

    reasoning_text = strip_protocol_tags(reasoning_text)
    full_result = f"<think>{reasoning_text}</think><answer>"
    update_task_stream(task_id, "", 'processing', truncate_result_delta(full_result), sync=True)

    answer_prompt = (
        f"{same_language_instruction} "
        "Using the reasoning below, output only the final answer to the user's request. "
        "Do not output reasoning. "
        "Do not use XML or tag wrappers. "
        "Start answering immediately.\n\n"
        f"Reasoning:\n{reasoning_text}\n\n"
        f"User request:\n{prompt}"
    )

    last_update_time = time.time()
    for token in iter_stream_tokens(model, answer_prompt, images, False, system_prompt, options):
        if time.time() - inference_start_time > MAX_TASK_RUNTIME:
            raise TaskTimeoutError("inference timed out")
        cancel_check_at = raise_if_task_cancelled(task_id, cancel_check_at)
        answer_text += token
        full_result = f"<think>{reasoning_text}</think><answer>{answer_text}"
        token_count += len(token)
        if time.time() - last_update_time > update_interval or len(token) >= 1:
            update_task_stream(task_id, "", 'processing', truncate_result_delta(full_result), sync=True)
            last_update_time = time.time()

    answer_text = strip_protocol_tags(answer_text)
    final_result = f"<think>{reasoning_text}</think><answer>{answer_text}</answer>"
    update_task_stream(task_id, "", 'processing', truncate_result_delta(final_result), sync=True)
    return final_result, token_count, first_token_ms


def terminate_inference_process(process, task_id: str):
    if not process:
        return
    if process.is_alive():
        log(f"WARN task {task_id[:8]} inference timeout, terminating worker process")
        process.terminate()
        process.join(timeout=3)
        if process.is_alive() and hasattr(process, "kill"):
            process.kill()
            process.join(timeout=2)


def run_inference_stream(task: dict, task_id: str, attempt: int = 0):
    model = resolve_task_model(task)
    if bool(task.get('deep_think', False)):
        return run_deep_think_stream(task, task_id, model, attempt=attempt)

    final_prompt, images, think_mode, system_prompt, language_hint = build_inference_payload(task, attempt=attempt)
    ollama_options = build_ollama_options(task, attempt=attempt)

    if is_code_task(task):
        return run_code_workflow(task, model, system_prompt, attempt=attempt)

    # On Windows + constrained VRAM, streaming subprocess path can return empty output.
    # Allow forcing non-stream direct generation for stability.
    if FORCE_NON_STREAM:
        result_text, token_count = ollama_generate_with_continuation(
            model, final_prompt, images, think_mode, system_prompt, ollama_options, language_hint=language_hint
        )
        return result_text, token_count, None

    full_result = ""
    token_count = 0
    first_token_sent = False
    first_token_ms = None
    last_update_time = time.time()
    pending_delta = ""
    think_mode_update_interval = 0.08 if think_mode else STREAM_UPDATE_INTERVAL
    think_mode_update_min_delta = 4 if think_mode else STREAM_UPDATE_MIN_DELTA
    inference_start_time = time.time()
    cancel_check_at = 0.0

    if os.name == "nt" and not FORCE_NON_STREAM:
        try:
            token_iterator = (
                iter_stream_tokens_http(model, final_prompt, images, think_mode, system_prompt, ollama_options)
                if not think_mode
                else iter_stream_tokens(model, final_prompt, images, think_mode, system_prompt, ollama_options)
            )
            for token in token_iterator:
                if time.time() - inference_start_time > MAX_TASK_RUNTIME:
                    raise TaskTimeoutError("inference timed out")
                cancel_check_at = raise_if_task_cancelled(task_id, cancel_check_at)
                full_result += token
                pending_delta = truncate_result_delta(pending_delta + token)
                token_count += len(token)
                if token and first_token_ms is None:
                    first_token_ms = max(1, int((time.time() - inference_start_time) * 1000))

                if not first_token_sent:
                    update_task_stream(task_id, "", 'processing', truncate_result_delta(full_result), sync=True, first_token_ms=first_token_ms)
                    first_token_sent = True
                    last_update_time = time.time()
                    pending_delta = ""
                elif (
                    time.time() - last_update_time > think_mode_update_interval
                    and len(pending_delta) >= think_mode_update_min_delta
                ):
                    update_task_stream(task_id, "", 'processing', truncate_result_delta(full_result), sync=True)
                    last_update_time = time.time()
                    pending_delta = ""
        except Exception as e:
            log(f"ERROR direct stream failed for task {task_id[:8]}: {e}")
            raise

        if pending_delta:
            update_task_stream(task_id, "", 'processing', truncate_result_delta(full_result), sync=True)

        if not full_result or not full_result.strip():
            fallback_options = dict(ollama_options)
            fallback_options["num_predict"] = max(128, int(fallback_options.get("num_predict", 128)))
            one_shot, fallback_token_count = ollama_generate_with_continuation(
                model, final_prompt, images, think_mode, system_prompt, fallback_options, language_hint=language_hint
            )
            if one_shot and one_shot.strip():
                full_result = one_shot
                token_count = max(token_count, fallback_token_count)

        return full_result, token_count, first_token_ms

    event_queue = MP_CONTEXT.Queue(maxsize=64)
    inference_process = MP_CONTEXT.Process(
        target=ollama_generate_worker,
        args=(model, final_prompt, images, think_mode, system_prompt, ollama_options, event_queue),
        daemon=True
    )
    inference_process.start()
    done_received = False

    try:
        while True:
            if time.time() - inference_start_time > MAX_TASK_RUNTIME:
                raise TaskTimeoutError("inference timed out")
            cancel_check_at = raise_if_task_cancelled(task_id, cancel_check_at)

            try:
                event = event_queue.get(timeout=0.5)
            except queue.Empty:
                if not inference_process.is_alive():
                    break
                continue

            event_type = event.get("type")
            if event_type == "done":
                done_received = True
                break
            if event_type == "error":
                raise MinerModelError(event.get("error", "unknown ollama error"))
            if event_type != "chunk":
                continue

            token = event.get('token', '')
            full_result += token
            pending_delta = truncate_result_delta(pending_delta + token)
            if token:
                token_count += len(token)
                if first_token_ms is None:
                    first_token_ms = max(1, int((time.time() - inference_start_time) * 1000))

            # 濠?token 缂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸婂潡鏌ㄩ弴鐐测偓鎼佹嫅閻斿吋鐓忓┑鐐靛亾濞呮捇鏌℃担绋款伃闁哄本绋戦埥澶愬础閻愯尙顔掗梻浣告惈濡酣宕愬┑瀣摕婵炴垯鍨归悞娲煕閹板吀绨存俊鎻掔墢缁辨挻鎷呴崫鍕戯絽鈹戦悙璇ц含鐎殿喖顭峰鎾偄閾忚鍟庨梻浣虹帛閸旓箓宕滃顒夌唵闁圭儤顨嗛埛鎴犵磼鐎ｎ偒鍎ラ柛搴㈠姍閺岀喎顫㈢仦钘夋優缂備緡鍣崣鍐箖閵忋倕浼犻柕澹懏顫岄梻鍌欑閹测€趁洪敃鍌氱煑閹肩补鍨鹃敐澶嬪€婚柤鎭掑劗閹峰姊虹粙鎸庢拱闁荤啙鍛濞寸厧鐡ㄩ悡鏇㈡煙閹屽殶闁瑰啿娲弻鐔碱敊閻偒浜崺鐐哄箣閻橆偄浜鹃柨婵嗙凹缁ㄥ鏌涚€ｎ亞效婵﹥妞藉Λ鍐ㄢ槈濮橆剦鏆俊鐐€х€靛矂宕瑰畷鍥у灊闁割偁鍎遍柋鍥煃閸ㄦ稒娅呭ù婊呭亾椤ㄣ儵鎮欓懠顑胯檸闂佸憡姊圭喊宥囨崲濞戙垹绾ч柟瀛樼妇閸嬫捇宕烽娑樹壕闂傚牊绋忛崑銏⑩偓瑙勬礃鐢繝骞冨▎鎴濆灊閻熸瑥瀚娲⒒閸屾瑧绐旈柍褜鍓涢崑娑㈡嚐椤栨稒娅犻悗娑欙供濞堜粙鏌ｉ幇顓熺稇濠殿喖鐗撻弻鐔碱敍濞戞瑯妫冩繝娈垮枓閸嬫捇姊洪崘鍙夋儓闁稿﹥鍔曢埞鎴犫偓锝庡亐閹峰姊虹粙鎸庢拱闁煎綊绠栭崺鈧い鎺嶇劍閸婃劗鈧娲橀崝娆撶嵁閺嶃劍濯撮柣鐔碱暒濡叉劙姊绘笟鈧褔鈥﹂崼銉ョ？婵炲棗绻嗗Σ鍫ユ煏韫囨洖顫嶇憸宥夆€︾捄銊﹀磯闁绘碍娼欐慨娑㈡⒑缂佹ɑ灏伴柛銊ユ健楠炲啫螖閸涱垰绁﹂梺鍓茬厛閸犳牗鎱ㄦ惔鈽嗘富闁靛牆绻掗悾铏繆椤愩垹顏柛娆忔噹椤啴濡堕崱娆忊拡闂佺顑囬崑銈夊箖?            if not first_token_sent:
                update_task_stream(task_id, "", 'processing', truncate_result_delta(full_result), sync=True, first_token_ms=first_token_ms)
                first_token_sent = True
                last_update_time = time.time()
                pending_delta = ""

            # 闂傚倸鍊搁崐鎼佸磹妞嬪海鐭嗗〒姘ｅ亾妤犵偛顦甸弫鎾绘偐閸愬弶鐤勯梻浣筋嚃閸ㄥジ鎮橀幇顖樹汗闁圭儤鎸搁埀顒€顭烽弻銈夊箒閹烘垵濮庢繛瀵稿Х閸庛倗鎹㈠┑瀣仺闂傚牊鍒€閿濆鐓犻柣鐔告緲閳锋梻绱掗纰辩吋鐎殿喛娉涢埢搴ㄥ礈瑜嶉弫鎼佹⒑閼姐倕鞋婵℃ぜ鍔庨幏鍐晝閸屾氨锛熼梺褰掑亰閸樿绂嶅鍫熸櫗濠电姵鑹鹃悙濠囨煏婵炲灝鐏悗姘矙濮婄粯鎷呮笟顖滃姼闂佸搫鐗滈崜娑氬垝濞嗘挸绠抽柡鍐ㄥ€婚ˇ顖炴⒑绾懏褰ч梻鍕閹€斥枎閹惧鍙勫┑顔斤供閸撴瑩鍩€椤掑偆鐒鹃柣锝囨暬瀹曞崬鈻庣仦鎴掑闁荤喐鐟ョ€氼厾绮堥崘顔界叆闁哄洦锚閻忊晠鏌ｉ敐鍥у幋濠碘剝鎮傞崺鈩冪節閸愨晜鐝濋梺鑽ゅ枑缁瞼绮旈悷閭﹀殨闁哄被鍎查弲鎻掝熆鐠轰警鍎岄柟閿嬫そ濮婃椽宕ㄦ繝鍕ㄦ闂佹寧娲╃粻鎾荤嵁婵犲洤绀嬫い鏍ㄧ〒閸欏棗鈹戦绛嬬劸闁糕晜鐗犲畷鎰版偨閸涘﹦鍘卞銈庡幗閸ㄥ灚绂嶉悙鐑樼厱婵炲棗鑻禍楣冩⒑鐠囧弶鍞夋い顐㈩槸鐓ら柍鍝勫暙缁躲倕霉閻樺樊鍎愰柛瀣ф櫆缁绘繈妫冨☉鍗炲壈缂備讲鍋撻悗锝庡亖娴滄粓鏌熼弶鍨暢缂佸娼ч湁闁绘ɑ鐟ュú锕傚煕閹烘嚚褰掓晲閸涱喗鍎撻柣銏╁灠閻栧ジ寮婚悢鍝勬瀳闁告鍋橀崰濠囨倵鐟欏嫭绀冮柛鏃€鐟ラ悾鐑芥倻缁涘鏅ｉ梺缁橆焽閺佹悂宕濋幖浣光拻濞达絿鎳撻婊勭箾閸欏澧电€规洘鍔橀妵鎰板箳閹寸姵鐓ｆ俊鐐€栫敮鎺楀磹閻㈢纾婚柟鎹愵嚙缁€鍌氼熆鐠虹尨姊楀瑙勬礋濮婄粯鎷呴崨濠傛殘濠电偠顕滈梽鍕矉瀹ュ鍊烽柛顭戝亽濞肩喎鈹戦绛嬬劸闁糕晜鐗犻崺娑㈠箣閻樼數锛滈柣搴秵閸樼晫娑甸崜浣虹＜闁绘ê鍟块ˉ瀣磼鏉堛劌绗氱€垫澘瀚换婵嬪礋椤撳鍔戝铏规嫚閳ヨ櫕鐏撻梺杞扮椤兘濡存担绯曟瀻闁规崘娅曢ˉ婵嬫⒑闂堟稓澧曟繛灞傚姂閹繝骞嬮敂瑙ｆ嫽婵炶揪绲块崕銈夊吹閳ь剟姊虹€癸附婢樻俊鍧楁煙楠炲灝鐏茬€规洘甯￠幃娆撴嚑椤掍胶鍙勯梻鍌欑缂嶅﹤螞閸ф鍊块柨鏇炲€归崑鍌炴煟閺傚灝鎮戦柣鎾寸〒閳ь剝顫夐幖鈺呭窗閺嶎偀鍋撳顒€妲婚摶鐐烘煥閻斿搫校闁抽攱鍨块弻娑樷攽閸℃浠惧銈冨劤婵炩偓闁哄本鐩幃鈺佺暦閸パ€鎷伴梻浣虹帛娓氭宕抽敐澶屽祦閻庯綆鍠楅弲婊呯磽娴ｈ偂鎴濃枍閹剧粯鈷掑ù锝囶焾閹垹绱掓担瑙勫唉鐎殿喗褰冮…銊╁醇閻斿嘲澹勯梻浣侯攰閹活亪姊介崟顖氱厱?
            elif (
                time.time() - last_update_time > think_mode_update_interval
                and len(pending_delta) >= think_mode_update_min_delta
            ):
                update_task_stream(task_id, "", 'processing', truncate_result_delta(full_result), sync=True)
                last_update_time = time.time()
                pending_delta = ""
        if not done_received and inference_process.exitcode not in (0, None):
            raise MinerModelError(f"闂傚倸鍊搁崐鎼佸磹瀹勬噴褰掑炊椤掑﹦绋忔繝銏ｆ硾椤戝洭銆呴幓鎹楀綊鎮╁顔煎壈缂備讲鍋撳鑸靛姇缁犺绻涢敐搴″濠德ゅ亹缁辨帡鎮╁畷鍥р吂闂佸疇顫夐崹鍧楀箖閳哄懎绠甸柟鐑樼箑濡叉劙姊绘担渚劸妞ゆ垵妫濋獮鎰板箹娴ｅ摜鍙€婵犮垼娉涜墝闁哄閰ｉ悡顐﹀炊閵婏附鍎庡┑鐐插悑閻熝呮閹捐纾兼繛鍡樺笒閸橈紕绱撴笟鍥ф珮闁搞劌鐖奸崹楣冩晝閸屾氨鍊為梺瀹犳〃缁€渚€鎮樻繝鍌楁斀闁绘劕寮堕ˉ鐐烘煕閵娧冩灈鐎规洘鍨块獮妯肩磼濡粯鐝抽梺鍦帶閻°劎鎹㈤崟顖氭瀬闁哄稁鍋嗙壕濂告煙椤栧棗鍟扮粙蹇曠磽娓氬洤鏋熼柣鐔叉櫊閻涱噣宕橀埡鍐炬祫闁诲函绲介悘姘跺疾椤掆偓閳规垿鎮欓弶鎴犱桓闂佽崵鍠嗛崕鐢稿春閳ь剚銇勯幒宥囶槮濠殿喖绉归弻锛勪沪閻ｅ睗銉︺亜瑜岀欢姘跺蓟濞戞粎鐤€闁哄啯鎹侀埀顒冩硶閳ь剝顫夊ú姗€宕归崸妤冨祦婵☆垵鍋愮壕鍏间繆椤栨粌甯舵鐐茬墦濮婄粯鎷呴悜妯烘畬濡炪倖娲﹂崢浠嬪箞閵娾晛绠绘い鏃囨閸擃參姊洪悷閭﹀殶濠殿噣顥撴竟鏇熺附缁嬭法楠囬梺鍓插亝缁嬫垶淇婇崸妤佺厱闁圭儤鎸剧瘬it_code={inference_process.exitcode}")
    except TaskTimeoutError:
        terminate_inference_process(inference_process, task_id)
        raise
    except Exception as e:
        log(f"闂?Ollama stream error: {e}")
        terminate_inference_process(inference_process, task_id)
        raise
    finally:
        if inference_process.is_alive():
            inference_process.join(timeout=1)
        try:
            event_queue.close()
        except Exception:
            pass

    if pending_delta:
        update_task_stream(task_id, "", 'processing', truncate_result_delta(full_result), sync=True)

    # Fallback: if stream path produced no visible answer, try one non-stream request.
    if not full_result or not full_result.strip():
        try:
            fallback_options = dict(ollama_options)
            fallback_options["num_predict"] = max(128, int(fallback_options.get("num_predict", 128)))
            one_shot, fallback_token_count = ollama_generate_with_continuation(
                model, final_prompt, images, think_mode, system_prompt, fallback_options, language_hint=language_hint
            )
            if one_shot and one_shot.strip():
                full_result = one_shot
                token_count = max(token_count, fallback_token_count)
        except Exception as e:
            log(f"WARN non-stream fallback failed for task {task_id[:8]}: {e}")

    return full_result, token_count, first_token_ms


def get_task_flag_str(task: dict) -> str:
    flags = []
    if task.get('image_url'):
        flags.append("image")
    if task.get('deep_think', False):
        flags.append("deep_think")
    if task.get('context'):
        flags.append("context")
    return " | ".join(flags) if flags else "standard"


def get_pipeline_snapshot() -> str:
    with inflight_tasks_lock:
        active_count = len(inflight_tasks)
    return (
        f"active={active_count}/{INFERENCE_WORKERS} | "
        f"claimed={claimed_task_queue.qsize()}/{claimed_task_queue.maxsize} | "
        f"submit={completion_queue.qsize()}/{completion_queue.maxsize} | "
        f"slots={OUTSTANDING_TASK_LIMIT - completion_queue.qsize() - claimed_task_queue.qsize() - active_count}/{OUTSTANDING_TASK_LIMIT}"
    )


def register_inflight_task(task_id: str, worker_index: int):
    with inflight_tasks_lock:
        inflight_tasks[task_id] = {
            "worker_index": worker_index,
            "started_at": time.time()
        }


def unregister_inflight_task(task_id: str):
    with inflight_tasks_lock:
        inflight_tasks.pop(task_id, None)


def claim_loop():
    idle_count = 0
    heartbeat_timer = 0

    while True:
        try:
            acquired_slot = task_slot_semaphore.acquire(timeout=CLAIM_QUEUE_FULL_SLEEP)
            if not acquired_slot:
                continue

            if time.time() - heartbeat_timer > 60:
                refresh_installed_ollama_models()
                gateway_request("heartbeat", {
                    "tier": HARDWARE_LEVEL,
                    "vram": VRAM_GB,
                    "installed_models": sorted(INSTALLED_OLLAMA_MODELS),
                })
                heartbeat_timer = time.time()

            if claimed_task_queue.full():
                task_slot_semaphore.release()
                time.sleep(CLAIM_QUEUE_FULL_SLEEP)
                continue

            if not is_auto_claim_enabled():
                task_slot_semaphore.release()
                time.sleep(CLAIM_IDLE_SLEEP)
                continue

            response_json = gateway_request("claim", {
                "score_limit": MODEL_SET['score'],
                "can_see": True,
                "mode": get_env("HEPH_TARGET_MODE", "TARGET_MODE") or "all",
                "accepted_sources": ACCEPTED_SOURCES,
                "excluded_sources": EXCLUDED_SOURCES,
            })

            if not response_json:
                task_slot_semaphore.release()
                log("闂傚倸鍊搁崐鎼佸磹妞嬪海鐭嗗〒姘ｅ亾闁诡喖娼″畷鎯邦槷闁哄鐗犻弻锟犲炊閳轰焦鐎婚梺鎼炲妽濡啴骞冨Δ鍛棃婵炴垶鐟﹂崰鎰磽?缂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮闁汇値鍠楅妵鍕冀椤愵澀绮堕梺鎼炲妼閸婂潡寮婚敐澶婎潊闁靛繆鏅濋崝鎼佹⒑濞茶骞楁い銊ョ墦楠炲骞栨担鍛婎棟濠电偛妫欓崕鍐测枔閵娿儺娓婚柕鍫濇绾剧敻鏌涚€ｎ偅宕岄柡宀€鍠栭、娆戞喆閸曨剛褰嬮柣搴ゎ潐濞测晝寰婇幆褜鍤楅柛鏇ㄥ幐閸嬫捇鏁愭惔婵堢泿婵犵鈧偨鍋㈡慨濠勭帛閹峰懘宕ㄦ繝鍛攨闂備礁鎲￠…鍡涘炊閵娿儰妲愰梻渚€娼ч…鍫ュ磿濞嗗繆妲堥柕蹇曞Х椤撴椽姊虹紒妯曟垿宕滃顒夌劷闁绘柨鐨濋弨浠嬫煟濡偐甯涙繛鎳峰嫮绠鹃柡澶嬪灥椤忣偅淇婇崣澶婂闁轰焦鍔栧鍕偓锝庡墮楠炲牓姊绘担瑙勭伇闁哄懏鐩畷鏉款潩椤戣姤鐏侀梺鍝勫暙閸婅崵澹曟總鍛婄厾闁艰婢橀悡鎰棯椤撶偛顣崇紒杈ㄥ笚濞煎繘濡搁妷锕佺檨婵°倗濮烽崑鐐哄垂閸ф宓侀柛銉墻閺佸洭鏌ｅ鍡楁灀闁稿鎸荤换婵嬪炊閵娿垺瀚奸梻浣瑰劤濞存岸宕戦崨杈剧稏婵°倕鎳忛悡鏇㈡倵閿濆骸浜滃┑顔碱樀閺屾盯骞掗幘铏癁閻庤娲栫紞濠囥€侀弴銏狀潊闁靛繈鍨诲畵浣圭節閻㈤潧啸闁轰礁鎲￠幈銊р偓鐢电《閸嬫挸顫濋悡搴㈢彎濡炪們鍨洪幑鍥春閳ь剚銇勯幒鎴濐仾闁抽攱甯￠弻娑氫沪閸撗勫櫘闂佸憡鏌ㄧ粔鍫曞箟閹间礁绾ч柛顭戝枟濞堝墎绱撴笟鍥ф灍婵☆偄鍟撮獮鍐煥閸喎娈熼梺闈涱槶閸庤鲸鏅?..")
                time.sleep(RUNTIME_ERROR_SLEEP)
                continue

            if response_json.get("status") == "idle":
                idle_count += 1
                task_slot_semaphore.release()
                if idle_count == 1:
                    log("婵犵數濮烽。钘壩ｉ崨鏉戠；闁规崘娉涚欢銈吤归悩宸剰闁汇値鍠楅妵鍕冀閵娿儺妫滈梺?闂傚倸鍊搁崐鎼佸磹妞嬪海鐭嗗〒姘ｅ亾妤犵偞鐗犻、鏇㈠煑閼恒儳鈽夐摶鏍煕濞戝崬骞橀柨娑欑懇濮婃椽鎳￠妶鍛€鹃柣搴㈣壘閻楀棝鍩ユ径鎰潊闁绘ɑ鐖犻崶銊у幈闂佹枼鏅涢崯浼村箠閹邦厾绠鹃柡澶嬪灥椤忣參鏌″畝瀣М闁诡喒鏅犲畷锝嗗緞婵犲孩袩闂佽崵鍠愮划宥咁熆濮椻偓瀹曨垶骞橀鑺ユ珳闂佺粯鍔曢幖顐㈡纯濠电姰鍨煎▔娑㈩敄閸ヮ剙鐭楅柛鈩冦仜閺€浠嬫煟閹邦厽缍戦柣蹇ラ檮閵囧嫰顢橀悙鏉戞灎闂佽鍠氶崑銈呯暦閵婏妇绠惧璺烘憸閻╁酣姊绘担鐟邦嚋缂佽鍊归〃銉╁川婵犲嫷娲搁梺瑙勵問閸犳帡宕戦幘鑸靛枂闁告洦鍓涢埞娑氱磽閸屾氨小缂佲偓娓氣偓椤㈡岸鏁愰崱娆樻祫闁诲函绲介悘姘跺疾濠靛鈷戦梻鍫熺洴閻涙粎绱掓潏銊︾鐎殿喗鎮傚畷姗€顢旈崱娆欑闯闂備胶顭堥張顒勬偡瑜旇棟闁挎柨顫曟禍婊堟煙鐎涙绠栭柛銈呮处閵囧嫰濮€閿涘嫬顫ч悗鍨緲鐎氼厾鎹㈠┑鍥ㄥ劅闁挎繂鎳庢闂傚倸鍊风欢姘焽瑜旈幃褔宕卞銏＄洴椤㈡﹢鎮欓浣镐壕闁圭儤顨嗛崵瀣煕韫囨挻鎲哥紒鐘宠壘椤啴濡堕崱娆忣潷缂備浇顕ч崐鍦矉瀹ュ牄浜归柟鐑樻尵閸樼敻姊洪懝閭︽綈婵犮垺蓱缁傚秷銇愰幒鎾跺帗閻熸粍绮撳畷婊冣枎閹炬潙鈧埖鎱ㄥ鍡楀⒒闁绘柨妫欓幈銊ヮ渻鐠囪弓澹曟俊銈囧Х閸嬫盯鏁冮妶澶樻晣闁稿繒鍘х欢鐐烘倵閿濆簼绨奸柣?..")
                time.sleep(CLAIM_IDLE_SLEEP)
                continue

            if response_json.get("status") != "success":
                task_slot_semaphore.release()
                log(f"闂傚倸鍊搁崐鎼佸磹妞嬪海鐭嗗〒姘ｅ亾闁诡喖娼″畷鎯邦槷闁哄鐗犻弻锟犲炊閳轰焦鐎婚梺鎼炲妽濡啴骞冨Δ鍛棃婵炴垶鐟﹂崰鎰磽?濠电姷鏁告慨鐑姐€傞挊澹╋綁宕ㄩ弶鎴狅紱闂佸憡渚楅崣搴ㄦ偄閸℃ü绻嗘い鏍ㄧ懅缁夋寧绻涢幋鐑嗙劯闁哄啫鐗嗙粈瀣煃鐞涒€充壕濠殿喛顫夐〃濠傤潖濞差亜绠归柣鎰絻婵矂姊洪崨濠冪叆闂佸府缍佹俊鎾川鐎涙ê鈧鏌ら幁鎺戝姎濞寸媭鍨跺娲箹閻愭彃濮岄梺鍛婃煥閻厧顕ユ繝鍕＜婵☆垶鏅茬花璇差渻閵堝棗濮夊┑顔芥尦閸┾偓妞ゆ帊鐒︾粈瀣煃閵夘垳鐣电€规洜顭堣灃闁逞屽墰缁顫濋懜鐢靛幍濠电偛鐗嗛悘婵嬪几閵堝洨纾介柛顐犲劙閹查箖鏌? {response_json}")
                time.sleep(RUNTIME_ERROR_SLEEP)
                continue

            idle_count = 0
            task = response_json.get("task_data")
            if not task or not task.get('id'):
                task_slot_semaphore.release()
                time.sleep(CLAIM_QUEUE_FULL_SLEEP)
                continue

            try:
                claimed_task_queue.put(task, timeout=1)
            except queue.Full:
                task_slot_semaphore.release()
                time.sleep(CLAIM_QUEUE_FULL_SLEEP)
                continue
            log(
                f"婵犵數濮烽。钘壩ｉ崨鏉戠；闁规崘娉涚欢銈吤归悩宸剰闁汇値鍠楅妵鍕冀椤愵澀绮剁紓?濠电姷鏁告慨鐑藉极閹间礁纾绘繛鎴欏焺閺佸銇勯幘璺烘瀾闁告瑥绻愯灃闁挎繂鎳庨弸銈夋煛娴ｅ壊鍎戦柟鎻掓啞閹棃濡搁妷褏鏉介梻渚€娼ц墝闁哄懏绮撳畷鎴﹀礋椤栨稓鍘介梺瑙勫礃濞呮洟骞戦敐鍡愪簻閿滃宕堕妸銏″闂傚倸鍊搁悧濠冪瑹濡も偓鍗遍柟缁㈠枟閻撴盯鏌涘☉鍗炴灓闁活厼瀛╅妵? {task['id'][:8]} | {get_task_flag_str(task)} | "
                f"{get_pipeline_snapshot()}"
            )
        except Exception as e:
            try:
                task_slot_semaphore.release()
            except ValueError:
                pass
            log(f"婵犵數濮烽。钘壩ｉ崨鏉戠；闁规崘娉涚欢銈吤归悩宸剰闁汇値鍠楅妵鍕冀椤愵澀娌梺?濠电姷鏁告慨鐑姐€傞挊澹╋綁宕ㄩ弶鎴狅紱闂佸憡渚楅崣搴ㄦ偄閸℃ü绻嗘い鏍ㄧ懅缁夋寧绻涢幋鐑嗙劯闁哄啫鐗嗙粈瀣煃鐞涒€充壕濠殿喛顫夐〃濠傤潖濞差亜绠归柣鎰絻婵姊洪崫鍕櫤缂佽鍊介悘瀣渻閵堝棛澧遍柛瀣仱瀹曟垿鏁愭径瀣幍濡炪倖鎸鹃崰搴ｇ箔閹烘垟鏀介柨娑樺閸樻挳鏌″畝瀣？濞寸媴濡囬幏鐘裁圭€ｎ亙澹曞┑鐘绘涧濡厼顭囬弽銊х鐎瑰壊鍠曠花鑽ょ磼閻樺崬宓嗘鐐寸墬濞煎繘宕滆钃遍梻渚€娼荤紞鍡涘垂娴肩补鈧? {e}")
            time.sleep(RUNTIME_ERROR_SLEEP)


def inference_worker_loop(worker_index: int):
    while True:
        task = claimed_task_queue.get()
        task_id = task.get('id', 'unknown')
        start_time = time.time()

        try:
            register_inflight_task(task_id, worker_index)
            update_task_stream(task_id, "", "processing", sync=True)

            log(f"闂傚倸鍊搁崐鎼佸磹瀹勬噴褰掑炊瑜夐弸宥夋煛閸モ晛袥闁稿鎸剧划顓炩槈濡顦╅梺绋款儜缁绘繂顕ｉ崼鏇為唶婵﹩鍘介悵鏍磽?闂傚倸鍊搁崐鎼佸磹瀹勬噴褰掑炊椤掑﹦绋忔繝銏ｆ硾椤戝洭銆呴幓鎹楀綊鎮╁顔煎壈缂備讲鍋撳鑸靛姇缁犺绻涢敐搴″濠德ゅ亹缁辨帡鎮╁畷鍥р吂闂佸疇顫夐崹鍧楀箖閳哄懎绠甸柟鐑樼箑缁辨垶绻濈喊妯活潑闁稿甯″畷褰掑醇閺囩偟鐣洪悷婊呭鐢帞绮婚幒妤佺厵闁绘垶锚閻忋儵宕鐐粹拻?{worker_index} 闂傚倸鍊峰ù鍥敋瑜忛埀顒佺▓閺呯娀銆佸▎鎾冲唨妞ゆ挾鍋熼悰銉╂⒑閸︻厼鍔嬫い銊ユ噽婢规洘绻濆顓犲幍闂佺粯鍔﹂崜姘舵倶闁秵鐓涢柍褜鍓熼幊鐐哄Ψ閿濆嫮鐩庨梻浣告惈閸燁偊宕愰悽绋跨闁跨喓濮甸悡鏇㈠箹鏉堝墽绡€闁告瑥瀚伴弻鈥崇暆閳ь剟宕伴弽顓熷仒妞ゆ洍鍋撶€规洖缍婇、娆撳矗閵壯咁槱闂?{task_id[:8]} | {get_pipeline_snapshot()}")

            result_text, token_count, first_token_ms = run_inference_with_retry(task, task_id)
            completion_queue.put({
                "task_id": task_id,
                "success": True,
                "result_text": result_text,
                "token_count": token_count,
                "first_token_ms": first_token_ms,
                "elapsed_seconds": time.time() - start_time,
                "task": task,
            })
        except Exception as e:
            completion_queue.put({
                "task_id": task_id,
                "success": False,
                "error": str(e),
                "elapsed_seconds": time.time() - start_time
            })
        finally:
            unregister_inflight_task(task_id)
            claimed_task_queue.task_done()


def submit_loop():
    while True:
        completion = completion_queue.get()
        task_id = completion["task_id"]
        elapsed = format_elapsed(completion["elapsed_seconds"])

        try:
            if completion["success"]:
                result_text = completion["result_text"]
                token_count = completion["token_count"]
                first_token_ms = completion.get("first_token_ms")
                task_meta = completion.get("task") if isinstance(completion.get("task"), dict) else {}
                result_text = normalize_model_output(result_text, bool(task_meta.get("deep_think", False)))

                if not result_text or not result_text.strip():
                    log(f"闂傚倸鍊搁崐椋庣矆娓氣偓閹潡宕惰閺嬫牠鏌￠崶鈺佹瀻闁搞劍妫冮幃妤呮濞戞瑦鍠愮紓?濠电姷鏁告慨鐑姐€傞挊澹╋綁宕ㄩ弶鎴濈€銈呯箰閻楀棝鎮為崹顐犱簻闁瑰搫妫楁禍鍓х磼閸撗嗘闁告瑥鍟村畷娲焵椤掍降浜滈柟鐑樺灥椤忣亪鏌℃担浠嬪摵缂佺粯鐩幃鈩冩償椤旀儳鎮戦柣搴㈩問閸ｎ噣宕滈悢鑲╁祦闁归偊鍘介崕鐔兼煥濠靛棗顒㈢悮锝囩磽閸屾艾鈧悂宕愭搴ｇ焼濞撴埃鍋撻柟宕囧枛椤㈡盯鎮欓懠顒夊晣婵＄偑鍊栭崝褏绮婚幋锔藉殝鐟滅増甯楅崐鐢告煟閵忋垺鏆╅柕鍡楋躬閺屾稓鈧綆鍋呭畷宀€鈧娲滈崗姗€銆佸鈧幃銈嗘媴闁垮鐓曠紓鍌氬€搁崐宄懊归崶銊ｄ粓闁告縿鍎插畷鏌ユ煕椤愶絿绠ラ柛銈嗘礀闇夐柣妯烘▕閸庡繘鏌ｉ幇顒婃敾闁靛洤瀚粻娑㈠箻缂傚簺鍨介弻鈩冩媴缁涘缍堢紓浣虹帛缁嬫捇鍩€椤掑倹鏆╂い顓炵墦閻庨攱淇婇悙顏勨偓鏍р枖閿曞倸绀嬫い鎺嗗亾鐎殿喖娼″铏圭磼濡儵鎷诲銈庡幖閻楀繐鈻庨姀銈嗗€烽柛婵嗗妤犲洭姊洪崜鎻掍航闁稿瀚粋宥夘敍濠婂嫬浠?| 婵犵數濮烽弫鎼佸磻濞戙埄鏁嬫い鎾跺枑閸欏繐螖閿濆懎鏋ら柡浣割儑閹插憡鎯旈妸銉х杽? {task_id[:8]} | 闂傚倸鍊搁崐宄懊归崶顒€纾婚柟閭﹀幗濞呯姵淇婇妶鍛櫡闁逞屽墮閸熸潙鐣烽崡鐐嶆梹绻濋崘銊фВ? {elapsed}")
                    gateway_request("fail", {"id": task_id})
                    continue

                result_hash = calculate_hash(result_text)
                update_task_stream(task_id, "", 'processing', truncate_result_delta(result_text), sync=True)

                confirm = gateway_request("submit", {
                    "id": task_id,
                    "result": result_text,
                    "hash": result_hash,
                    "token_count": token_count,
                    "first_token_ms": first_token_ms,
                })

                if confirm and confirm.get("status") == "success":
                    log(f"OK task {task_id[:8]} submitted | elapsed: {elapsed} | {get_pipeline_snapshot()}")
                else:
                    message = "gateway no response"
                    if confirm:
                        message = confirm.get('message', 'unknown reason')
                    log(f"ERROR task {task_id[:8]} submit failed: {message} | elapsed: {elapsed}")
            else:
                error_text = str(completion["error"] or "")
                if "task cancelled by user" in error_text.lower():
                    log(f"INFO task {task_id[:8]} cancelled by user | elapsed: {elapsed}")
                    update_task_stream(task_id, "", 'cancelled', sync=True)
                    continue
                log(f"ERROR inference exception: {error_text} | task: {task_id[:8]} | elapsed: {elapsed}")
                gateway_request("fail", {"id": task_id})
        except Exception as e:
            log(f"婵犵數濮烽。钘壩ｉ崨鏉戠；闁规崘娉涚欢銈吤归悩宸剰闁汇値鍠楅妵鍕冀椤愵澀娌梺?闂傚倸鍊搁崐鎼佸磹妞嬪海鐭嗗ù锝夋交閼板潡寮堕崼姘珔闁搞劍绻冮妵鍕冀椤愵澀绮剁紓浣插亾濠㈣埖鍔栭悡娆撴煟閹寸伝顏堟倿閻愵剛绠鹃悘蹇旂墬濞呭﹪鏌＄仦璇插鐎殿噮鍓熷畷褰掝敊鐟欏嫬鐦卞┑鐘殿暯濡插懘宕戦幒妤€鍨傞柛顐ｆ礀缁犳牜鎲搁悧鍫濈瑨缂佺姵甯￠弻鐔兼倻濡櫣浠撮柛鐑嗗灦濮婄粯鎷呴崨濠冨創闁荤偞鍑归崑濠傜暦閹邦兘鏀介柛鈾€鏅滅紞搴♀攽閻愬弶鈻曞ù婊勭矌缁粯瀵奸弶鎴狀啇闁哄鐗嗘晶鐣岀矙婵犳碍鐓曢柨婵嗛閻忕姵銇? {e} | 濠电姷鏁告慨鐑藉极閹间礁纾绘繛鎴欏焺閺佸銇勯幘璺烘瀾闁告瑥绻愯灃闁挎繂鎳庨弸銈夋煛娴ｅ壊鍎戦柟鎻掓啞閹棃濡搁妷褏鏉? {task_id[:8]}")
        finally:
            task_slot_semaphore.release()
            completion_queue.task_done()


def format_elapsed(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    secs = seconds % 60
    return f"{minutes}m {secs:.1f}s"


def run_miner():
    log(f"婵犵數濮烽。钘壩ｉ崨鏉戠；闁规崘娉涚欢銈吤归悩宸剰闁汇値鍠楅妵鍕冀椤愵澀绮剁紓?[{MINER_NAME}] ONLINE | HWID: {HARDWARE_ID[:12]}")
    log(f"婵犵數濮烽。钘壩ｉ崨鏉戠；闁规崘娉涚欢銈吤归悩宸剰闁汇値鍠楅妵鍕冀椤愵澀绮剁紓?TIER: {HARDWARE_LEVEL} | VRAM: {VRAM_GB}GB | GPUS: {GPU_COUNT}")
    log(f"婵犵數濮烽。钘壩ｉ崨鏉戠；闁规崘娉涚欢銈吤归悩宸剰缂佹劖顨嗛幈銊ヮ潨閸℃濮?婵犵數濮烽弫鍛婃叏閻戝鈧倿鎸婃竟鈺嬬秮瀹曘劑寮堕幋婵堚偓顓烆渻閵堝懐绠伴柣妤€妫濋幃鐐哄垂椤愮姳绨婚梺鐟版惈濡绂嶉崜褏纾奸柛鎾楀棙顎楅梺鍛婄懃閸熸潙鐣峰ú顏勭劦? {MODEL_SET['cod']}")
    log(
        f"婵犵數濮烽。钘壩ｉ崨鏉戠；闁规崘娉涚欢銈吤归悩宸剰闁汇値鍠楅妵鍕冀椤愵澀绮跺┑?Ollama options | low_vram={LOW_VRAM_MODE} | force_non_stream={FORCE_NON_STREAM} | "
        f"num_ctx={OLLAMA_NUM_CTX} | num_predict={OLLAMA_NUM_PREDICT} | num_batch={OLLAMA_NUM_BATCH}"
    )
    log(f"婵犵數濮烽。钘壩ｉ崨鏉戠；闁规崘娉涚欢銈吤归悩宸剰闁?婵犵數濮烽弫鍛婃叏閻戣棄鏋侀柟闂寸绾惧鏌ｉ弮鍌氬妺鐎规洖寮剁换婵嬫濞戝崬鍓遍梺绋匡工濞硷繝寮婚妸鈺佸嵆闁绘劖绁撮崑鎾诲传閵壯傜瑝闂佸搫顦伴娆撴偄閸℃稒鍋ｅΔ锔藉椤忕娀鏌ｈ箛姘跺摵濞ｅ洤锕、鏇㈩敃閵忣澀妗撻梻浣告惈閺堫剛绮欓弽顓勫洭鎼归鐘辩盎闂侀潧顭堥崕鏌ュ闯娴犲鐓涘〒姘搐閺嬫盯鏌ｉ敐鍥у幋鐎规洖鐖奸崺锟犲礃鐠恒劑妫?| inference_workers={INFERENCE_WORKERS} | claim_queue={CLAIM_QUEUE_MAXSIZE}")

    if ACCEPTED_SOURCES or EXCLUDED_SOURCES:
        log(f"Source filters | accepted={ACCEPTED_SOURCES or ['*']} | excluded={EXCLUDED_SOURCES or []}")
    start_local_profile_server()
    check_ollama()
    start_stream_update_worker()
    threads = [
        threading.Thread(target=claim_loop, name="claim-loop", daemon=True),
        threading.Thread(target=submit_loop, name="submit-loop", daemon=True),
    ]

    for worker_index in range(1, INFERENCE_WORKERS + 1):
        threads.append(
            threading.Thread(
                target=inference_worker_loop,
                args=(worker_index,),
                name=f"inference-{worker_index}",
                daemon=True
            )
        )

    for thread in threads:
        thread.start()

    while True:
        time.sleep(30)
        log(f"婵犵數濮烽。钘壩ｉ崨鏉戠；闁规崘娉涚欢銈吤归悩宸剰闁汇値鍠楅妵鍕冀椤愵澀绮剁紓?闂傚倸鍊搁崐鎼佸磹閻戣姤鍊块柨鏇楀亾妞ゎ亜鍟村畷绋课旈埀顒勫磼閵娿儮鏀介柛灞剧閸熺偤鏌涙繝鍛厫缂佺粯绻堝Λ鍐ㄢ槈濞嗘劖鍊锋俊鐐€栧ú鐔哥閸洖钃熼柣鏂垮悑閻掍粙鏌ㄩ弴妤€浜惧Δ鐘靛仦椤ㄥ懘婀佸┑鐘欏啰浠㈤柍褜鍓氱换鍫濐嚕婵犳碍鍋勯柛蹇氬亹閸旂兘姊洪幐搴㈢５闁稿鎹囬弻锝堢疀濞戞鍠氶梺鍝勭焿缁绘繈宕洪埀顒併亜閹烘垵顏悗鐢靛Т椤法鎹勯悜姗嗘！濡?| {get_pipeline_snapshot()}")


if __name__ == "__main__":
    multiprocessing.freeze_support()
    run_miner()





