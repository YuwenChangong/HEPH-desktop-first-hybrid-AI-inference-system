const { useEffect, useMemo, useRef, useState } = React;

const API_BASE = "http://127.0.0.1:8000";
const LOCAL_MINER_PROFILE_URL = `${API_BASE}/orders/local-profile`;
const LOCAL_MINER_CONTROL_URL = "http://127.0.0.1:8765/miner-control";
const MODE_OPTIONS = ["Local", "Auto", "Remote"];
const PAGE_OPTIONS = [{ key: "chat" }, { key: "orders" }];
const DEFAULT_MODELS = ["qwen3.5:2b", "qwen3.5:9b", "qwen3.5:27b"];
const STORAGE_KEY = "ai-reasoning-model-options";
const USER_KEY = "ai-reasoning-user-id";
const AUTH_TOKEN_KEY = "ai-reasoning-auth-token";
const SUPABASE_ACCESS_TOKEN_KEY = "ai-reasoning-supabase-access-token";
const SUPABASE_REFRESH_TOKEN_KEY = "ai-reasoning-supabase-refresh-token";
const WORKER_KEY = "ai-reasoning-worker-name";
const AUTO_CLAIM_KEY = "ai-reasoning-auto-claim";
const CLAIM_CAPABILITY_KEY = "ai-reasoning-claim-capability";
const CHAT_SESSIONS_KEY = "ai-reasoning-chat-sessions";
const CURRENT_CHAT_KEY = "ai-reasoning-current-chat";
const THEME_KEY = "ai-reasoning-theme";
const LANG_KEY = "ai-reasoning-lang";
const MAX_CONTEXT_MESSAGES = 12;
const MAX_CONTEXT_CONTENT_CHARS = 900;
const MAX_IMAGE_BYTES = 5 * 1024 * 1024;
const LOCAL_NO_TOKEN_TIMEOUT_MS = 30 * 1000;
const CLAIM_CAPABILITY_OPTIONS = [
  { key: "auto", label: "Auto" },
  { key: "2b", label: "<= 2b" },
  { key: "9b", label: "<= 9b" },
  { key: "27b", label: "<= 27b" },
];
const ORDER_VIEW_OPTIONS = [{ key: "eligible" }, { key: "all" }, { key: "mine" }];
const LANGUAGE_OPTIONS = [
  { key: "en", label: "English" },
  { key: "zh", label: "中文" },
  { key: "ja", label: "日本語" },
  { key: "fr", label: "Français" },
];
const ORDER_VISIBLE_LIMIT = 30;
function normalizeSupabaseUrl(raw) {
  const text = String(raw || "").trim();
  if (!text) return "";
  return text.replace(/\/+$/, "").replace(/\/rest\/v1$/i, "");
}

function parseHashAuthParams(rawHash) {
  const hash = String(rawHash || "").replace(/^#/, "").trim();
  if (!hash) return {};
  const params = new URLSearchParams(hash);
  return {
    access_token: String(params.get("access_token") || "").trim(),
    refresh_token: String(params.get("refresh_token") || "").trim(),
    token_type: String(params.get("token_type") || "").trim(),
    error: String(params.get("error") || "").trim(),
    error_description: String(params.get("error_description") || "").trim(),
  };
}

function storeSupabaseSessionTokens(accessToken = "", refreshToken = "") {
  try {
    const nextAccess = String(accessToken || "").trim();
    const nextRefresh = String(refreshToken || "").trim();
    if (nextAccess) {
      localStorage.setItem(SUPABASE_ACCESS_TOKEN_KEY, nextAccess);
    } else {
      localStorage.removeItem(SUPABASE_ACCESS_TOKEN_KEY);
    }
    if (nextRefresh) {
      localStorage.setItem(SUPABASE_REFRESH_TOKEN_KEY, nextRefresh);
    } else {
      localStorage.removeItem(SUPABASE_REFRESH_TOKEN_KEY);
    }
  } catch {}
}

const SUPABASE_URL = normalizeSupabaseUrl(
  window.__SUPABASE_URL || localStorage.getItem("SUPABASE_URL") || localStorage.getItem("supabase_url") || ""
);
const SUPABASE_ANON_KEY =
  window.__SUPABASE_ANON_KEY ||
  localStorage.getItem("SUPABASE_ANON_KEY") ||
  localStorage.getItem("supabase_anon_key") ||
  "";

const I18N = {
  en: {
    page_chat: "Chat",
    page_orders: "Orders",
    theme_light: "Light",
    theme_dark: "Dark",
    credit: "Credit",
    no_local_models: "No local models",
    add_model_placeholder: "Add model",
    ollama_install_repair: "Install / Repair Ollama",
    ollama_runtime_ready: "Ollama ready",
    ollama_runtime_light: "Using bundled lightweight Ollama",
    ollama_runtime_missing: "Ollama not detected",
    ollama_installer_opened: "Ollama installer opened",
    ollama_install_failed: "Unable to start Ollama installer",
    model_cmd_title: "Type ollama list / ollama pull <model> / ollama rm <model>; plain model name only adds it to list",
    model_cmd_hint: "Type ollama list / ollama pull <model> / ollama rm <model>...",
    deep_think: "Deep Thinking",
    on: "On",
    off: "Off",
    composer_placeholder: "Type a prompt, Enter to send, Shift+Enter for newline",
    send: "Send",
    remove: "Remove",
    streaming_failed: "Request failed, please retry.",
    thinking: "Thinking...",
    thinking_done: "Thinking completed",
    generating: "Generating answer...",
    answer_format_error: "Answer format is invalid. Please regenerate.",
    expand: "Expand",
    collapse: "Collapse",
    retry: "Retry",
    copy: "Copy",
    regenerate: "Regenerate",
    new_chat: "New Chat",
    search_chat: "Search history",
    clear_current: "Clear current",
    export: "Export",
    no_chat_match: "No matching chat history.",
    pinned: "Pinned",
    pin: "Pin",
    unpin: "Unpin",
    rename: "Rename",
    delete: "Delete",
    empty_session: "Empty session",
    question_count: "{count} prompts",
    orders_title: "Orders",
    orders_subtitle: "For miners to view pending tasks, claim tasks, and track claimed history.",
    my_miner_identity: "My miner identity",
    worker_placeholder: "Enter miner name",
    worker_bound: "Bound to local miner_name automatically; edit this field to override.",
    worker_unbound: "This name is written into task.miner_name.",
    claim_config: "Claim config",
    auto_claim_on: "Auto-claiming",
    auto_claim_off: "Enable auto-claim",
    status_refreshing: "Refreshing orders...",
    status_synced: "Order list synced.",
    status_load_failed: "Load failed: {error}",
    success_rate: "Success rate",
    avg_first_token: "Avg first token",
    fail_top: "Top failure",
    mine_total: "Mine",
    mine_active: "Processing",
    mine_done_failed: "Completed / Failed",
    local_models_ready: "Local models ready",
    tier_27b: "27b tier",
    tier_9b: "9b tier",
    tier_2b: "2b tier",
    rec_16gb: "Recommended 16GB+ VRAM",
    rec_6gb: "Recommended 6GB+ VRAM",
    rec_4gb: "Recommended 4GB+ VRAM",
    view_eligible: "Eligible",
    view_all: "All",
    view_mine: "Mine",
    visible_mine_title: "My Claimed Tasks",
    visible_all_title: "All Pending Tasks",
    visible_eligible_title: "Eligible Tasks",
    visible_mine_desc: "Recent tasks under current miner identity.",
    visible_all_desc: "All frontend pending tasks, including tasks beyond current capacity.",
    visible_eligible_desc: "Only tasks eligible for current miner configuration.",
    empty_mine: "You have not claimed any tasks yet.",
    empty_orders: "No tasks match this filter.",
    claim_done: "Claimed",
    claim_ing: "Claiming...",
    claim_action: "Claim",
    over_capability: "Over capacity",
    model_missing: "Not installed",
    order_requester: "Requester",
    installed: "Installed locally",
    not_installed: "Not installed locally",
    unknown: "unknown",
    created_at: "Created",
    claimed_at: "Claimed",
    completed_at: "Completed",
    footer_orders: "Orders page is for task claiming and history. Switch back to Chat for conversation.",
    app_title: "HEPH",
    app_subtitle: "Minimal UI with expandable execution details.",
    rename_chat_prompt: "Rename chat",
    delete_chat_confirm: "Delete chat \"{title}\"? This action cannot be undone.",
    clear_chat_confirm: "Clear current conversation? This cannot be undone.",
    model_missing_notice: "No available local model detected. Add or install a model first.",
    mode_local: "Local",
    mode_auto: "Auto",
    mode_remote: "Remote",
    orders_word: "orders",
    eligible_suffix: "eligible",
    order_status_pending: "Pending",
    order_status_claimed: "Claimed",
    order_status_processing: "Processing",
    order_status_completed: "Completed",
    order_status_failed: "Failed",
    order_status_cancelled: "Cancelled",
    stop_generating: "Stop generation",
    uploaded_image_alt: "uploaded image",
    reasoning_std: "Standard",
    reasoning_deep: "Deep",
    cap_known_auto: "Current miner config: {vram}GB, default claim up to {label}.",
    cap_unknown_auto: "Current miner config unknown; Auto will not limit model tier for now.",
    cap_manual: "Manual limit set to <= {pref}.",
    cap_exceed_msg: "Exceeds current miner claim capability, cannot claim for now.",
    model_not_installed_msg: "Model is not installed locally, cannot claim for now.",
    current_miner: "Current miner",
    claimed_at_label: "Claimed at",
    completed_at_label: "Completed at",
    login_title: "Sign in",
    login_subtitle: "Use Google, GitHub, email, or phone to continue.",
    login_google: "Continue with Google",
    login_github: "Continue with GitHub",
    login_email_label: "Email",
    login_phone_label: "Phone",
    login_send_email: "Send email code",
    login_send_phone: "Send SMS code",
    login_otp_label: "OTP code",
    login_verify_email: "Verify email code",
    login_verify_phone: "Verify SMS code",
    login_sign_out: "Sign out",
    login_error_prefix: "Login failed: {error}",
    login_missing_cfg: "Missing Supabase config. Set window.__SUPABASE_URL and window.__SUPABASE_ANON_KEY.",
  },
  zh: {
    page_chat: "聊天",
    page_orders: "接单",
    theme_light: "浅色",
    theme_dark: "深色",
    credit: "Credit",
    no_local_models: "无本机模型",
    add_model_placeholder: "添加模型",
    ollama_install_repair: "安装 / 修复 Ollama",
    ollama_runtime_ready: "Ollama 已就绪",
    ollama_runtime_light: "当前使用内置轻量 Ollama",
    ollama_runtime_missing: "未检测到 Ollama",
    ollama_installer_opened: "已启动 Ollama 安装器",
    ollama_install_failed: "无法启动 Ollama 安装器",
    model_cmd_title: "输入 ollama list / ollama pull <model> / ollama rm <model>；普通模型名只加入列表",
    model_cmd_hint: "输入 ollama list / ollama pull <model> / ollama rm <model>...",
    deep_think: "深度思考",
    on: "开",
    off: "关",
    composer_placeholder: "输入 prompt，按 Enter 发送，Shift+Enter 换行",
    send: "发送",
    remove: "移除",
    streaming_failed: "请求失败，请重试。",
    thinking: "正在思考...",
    thinking_done: "已完成思考",
    generating: "正在生成回答...",
    answer_format_error: "回答格式异常，请点击重新生成。",
    expand: "展开",
    collapse: "折叠",
    retry: "重试",
    copy: "复制",
    regenerate: "重新生成",
    new_chat: "新建聊天",
    search_chat: "搜索历史聊天",
    clear_current: "清空当前",
    export: "导出",
    no_chat_match: "没有匹配的历史聊天。",
    pinned: "置顶",
    pin: "置顶",
    unpin: "取消置顶",
    rename: "重命名",
    delete: "删除",
    empty_session: "空会话",
    question_count: "{count} 条提问",
    orders_title: "接单页",
    orders_subtitle: "这里给想接任务的用户或矿工使用。你可以查看待接单列表、手动接单，并查看自己当前和历史接过的任务。",
    my_miner_identity: "我的接单身份",
    worker_placeholder: "输入你的接单名称",
    worker_bound: "已自动绑定到本机 miner_name；如需手动切换，可直接修改这个名字。",
    worker_unbound: "这个名字会写进任务的 miner_name 字段。",
    claim_config: "接单配置",
    auto_claim_on: "自动接单中",
    auto_claim_off: "开启自动接单",
    status_refreshing: "正在刷新接单列表...",
    status_synced: "接单列表已同步。",
    status_load_failed: "加载失败：{error}",
    success_rate: "成功率",
    avg_first_token: "平均首 Token",
    fail_top: "失败 Top",
    mine_total: "我已接",
    mine_active: "处理中",
    mine_done_failed: "已完成 / 失败",
    local_models_ready: "本机模型可用",
    tier_27b: "27b 档",
    tier_9b: "9b 档",
    tier_2b: "2b 档",
    rec_16gb: "建议 16GB+ VRAM",
    rec_6gb: "建议 6GB+ VRAM",
    rec_4gb: "建议 4GB+ VRAM",
    view_eligible: "只看可接",
    view_all: "全部任务",
    view_mine: "我已接",
    visible_mine_title: "我已接任务",
    visible_all_title: "全部任务",
    visible_eligible_title: "可接任务",
    visible_mine_desc: "展示当前接单身份名下最近的任务。",
    visible_all_desc: "展示前端来源的全部 pending 任务，包含超出当前配置的任务。",
    visible_eligible_desc: "只展示当前矿工配置下可接的任务。",
    empty_mine: "你还没有接到任何任务。",
    empty_orders: "当前没有符合该筛选条件的任务。",
    claim_done: "已接",
    claim_ing: "接单中...",
    claim_action: "接单",
    over_capability: "超出配置",
    model_missing: "未安装",
    order_requester: "发单人",
    installed: "本机已安装",
    not_installed: "本机未安装",
    unknown: "unknown",
    created_at: "创建于",
    claimed_at: "接单于",
    completed_at: "完成于",
    footer_orders: "接单页用于查看可接任务和我的任务。聊天请切回“聊天”页面。",
    app_title: "HEPH",
    app_subtitle: "极简界面，按需展开执行细节。",
    rename_chat_prompt: "重命名聊天",
    delete_chat_confirm: "删除聊天“{title}”后将无法恢复，确认继续吗？",
    clear_chat_confirm: "清空当前会话内容后无法恢复，确认继续吗？",
    model_missing_notice: "未检测到可用模型，请先添加或安装模型。",
    mode_local: "本机",
    mode_auto: "自动",
    mode_remote: "远程",
    orders_word: "单",
    eligible_suffix: "可接",
    order_status_pending: "待接单",
    order_status_claimed: "已接单",
    order_status_processing: "处理中",
    order_status_completed: "已完成",
    order_status_failed: "失败",
    order_status_cancelled: "已取消",
    stop_generating: "停止生成",
    uploaded_image_alt: "上传图片",
    reasoning_std: "标准",
    reasoning_deep: "深度",
    cap_known_auto: "当前矿工配置：{vram}GB，默认可接 {label} 及以下任务。",
    cap_unknown_auto: "当前矿工配置未知，Auto 暂时不会限制模型档位。",
    cap_manual: "当前手动限制为 {pref} 及以下任务。",
    cap_exceed_msg: "超出当前矿工接单配置，暂时不能接这条任务。",
    model_not_installed_msg: "本机未安装该模型，暂时不能接这条任务。",
    current_miner: "当前接单者",
    claimed_at_label: "接单于",
    completed_at_label: "完成于",
    login_title: "登录",
    login_subtitle: "使用 Google、GitHub、邮箱或手机号登录后继续。",
    login_google: "Google 登录",
    login_github: "GitHub 登录",
    login_email_label: "邮箱",
    login_phone_label: "手机号",
    login_send_email: "发送邮箱验证码",
    login_send_phone: "发送短信验证码",
    login_otp_label: "验证码",
    login_verify_email: "验证邮箱验证码",
    login_verify_phone: "验证短信验证码",
    login_sign_out: "退出登录",
    login_error_prefix: "登录失败：{error}",
    login_missing_cfg: "缺少 Supabase 配置，请设置 window.__SUPABASE_URL 和 window.__SUPABASE_ANON_KEY。",
  },
  ja: {
    page_chat: "チャット",
    page_orders: "受注",
    theme_light: "ライト",
    theme_dark: "ダーク",
    credit: "クレジット",
    no_local_models: "ローカルモデルなし",
    add_model_placeholder: "モデル追加",
    ollama_install_repair: "Ollama をインストール / 修復",
    ollama_runtime_ready: "Ollama 利用可能",
    ollama_runtime_light: "内蔵軽量 Ollama を使用中",
    ollama_runtime_missing: "Ollama が見つかりません",
    ollama_installer_opened: "Ollama インストーラーを起動しました",
    ollama_install_failed: "Ollama インストーラーを起動できません",
    model_cmd_title: "ollama list / ollama pull <model> / ollama rm <model> を入力して実行。通常のモデル名は一覧に追加のみ。",
    model_cmd_hint: "ollama list / ollama pull <model> / ollama rm <model> ...",
    deep_think: "深い思考",
    on: "オン",
    off: "オフ",
    composer_placeholder: "プロンプト入力。Enterで送信、Shift+Enterで改行",
    send: "送信",
    remove: "削除",
    streaming_failed: "リクエスト失敗。再試行してください。",
    thinking: "思考中...",
    thinking_done: "思考完了",
    generating: "回答生成中...",
    answer_format_error: "回答フォーマット異常。再生成してください。",
    expand: "展開",
    collapse: "折りたたみ",
    retry: "再試行",
    copy: "コピー",
    regenerate: "再生成",
    new_chat: "新しいチャット",
    search_chat: "履歴検索",
    clear_current: "現在をクリア",
    export: "エクスポート",
    no_chat_match: "一致する履歴がありません。",
    pinned: "固定",
    pin: "固定",
    unpin: "固定解除",
    rename: "名前変更",
    delete: "削除",
    empty_session: "空の会話",
    question_count: "{count} 件の質問",
    orders_title: "受注ページ",
    orders_subtitle: "マイナー向け。待機タスクの確認、手動受注、履歴確認ができます。",
    my_miner_identity: "マイナーID",
    worker_placeholder: "マイナー名を入力",
    worker_bound: "ローカル miner_name に自動紐付け済み。必要なら直接変更できます。",
    worker_unbound: "この名前はタスクの miner_name に保存されます。",
    claim_config: "受注設定",
    auto_claim_on: "自動受注中",
    auto_claim_off: "自動受注を有効化",
    status_refreshing: "受注リスト更新中...",
    status_synced: "受注リスト同期済み。",
    status_load_failed: "読み込み失敗: {error}",
    success_rate: "成功率",
    avg_first_token: "平均 First Token",
    fail_top: "失敗Top",
    mine_total: "受注合計",
    mine_active: "処理中",
    mine_done_failed: "完了 / 失敗",
    local_models_ready: "ローカル利用可能モデル",
    tier_27b: "27b帯",
    tier_9b: "9b帯",
    tier_2b: "2b帯",
    rec_16gb: "推奨 16GB+ VRAM",
    rec_6gb: "推奨 6GB+ VRAM",
    rec_4gb: "推奨 4GB+ VRAM",
    view_eligible: "受注可能のみ",
    view_all: "すべて",
    view_mine: "自分の受注",
    visible_mine_title: "自分の受注タスク",
    visible_all_title: "全タスク",
    visible_eligible_title: "受注可能タスク",
    visible_mine_desc: "現在のマイナーIDに紐づく最新タスクを表示。",
    visible_all_desc: "frontend 起点の pending タスクをすべて表示。",
    visible_eligible_desc: "現在のマイナー構成で受注可能なタスクのみ表示。",
    empty_mine: "まだ受注したタスクはありません。",
    empty_orders: "条件に一致するタスクがありません。",
    claim_done: "受注済み",
    claim_ing: "受注中...",
    claim_action: "受注",
    over_capability: "構成超過",
    model_missing: "未インストール",
    order_requester: "依頼者",
    installed: "ローカル導入済み",
    not_installed: "ローカル未導入",
    unknown: "unknown",
    created_at: "作成",
    claimed_at: "受注",
    completed_at: "完了",
    footer_orders: "受注ページです。会話はチャットページに戻ってください。",
    app_title: "HEPH",
    app_subtitle: "ミニマルUI、必要時のみ詳細表示。",
    rename_chat_prompt: "チャット名を変更",
    delete_chat_confirm: "チャット「{title}」を削除します。元に戻せません。続行しますか？",
    clear_chat_confirm: "現在の会話をクリアします。元に戻せません。続行しますか？",
    model_missing_notice: "利用可能なモデルがありません。先に追加またはインストールしてください。",
    mode_local: "ローカル",
    mode_auto: "自動",
    mode_remote: "リモート",
    orders_word: "件",
    eligible_suffix: "件可",
    order_status_pending: "待機中",
    order_status_claimed: "受注済み",
    order_status_processing: "処理中",
    order_status_completed: "完了",
    order_status_failed: "失敗",
    order_status_cancelled: "キャンセル",
    stop_generating: "生成停止",
    uploaded_image_alt: "アップロード画像",
    reasoning_std: "標準",
    reasoning_deep: "深い思考",
    cap_known_auto: "現在のマイナー構成: {vram}GB、既定で {label} 以下を受注可能。",
    cap_unknown_auto: "現在のマイナー構成は不明です。Auto は一時的にモデル帯域を制限しません。",
    cap_manual: "手動上限: <= {pref}",
    cap_exceed_msg: "現在のマイナー構成上限を超えるため受注できません。",
    model_not_installed_msg: "モデル未インストールのため受注できません。",
    current_miner: "現在の受注者",
    claimed_at_label: "受注時刻",
    completed_at_label: "完了時刻",
    login_title: "ログイン",
    login_subtitle: "Google / GitHub / メール / 電話でログインしてください。",
    login_google: "Googleでログイン",
    login_github: "GitHubでログイン",
    login_email_label: "メール",
    login_phone_label: "電話番号",
    login_send_email: "メールコード送信",
    login_send_phone: "SMSコード送信",
    login_otp_label: "認証コード",
    login_verify_email: "メールコード確認",
    login_verify_phone: "SMSコード確認",
    login_sign_out: "ログアウト",
    login_error_prefix: "ログイン失敗: {error}",
    login_missing_cfg: "Supabase 設定がありません。window.__SUPABASE_URL と window.__SUPABASE_ANON_KEY を設定してください。",
  },
  fr: {
    page_chat: "Chat",
    page_orders: "Tâches",
    theme_light: "Clair",
    theme_dark: "Sombre",
    credit: "Crédit",
    no_local_models: "Aucun modèle local",
    add_model_placeholder: "Ajouter un modèle",
    ollama_install_repair: "Installer / Réparer Ollama",
    ollama_runtime_ready: "Ollama prêt",
    ollama_runtime_light: "Ollama léger intégré utilisé",
    ollama_runtime_missing: "Ollama introuvable",
    ollama_installer_opened: "Installateur Ollama lancé",
    ollama_install_failed: "Impossible de lancer l’installateur Ollama",
    model_cmd_title: "Saisir ollama list / ollama pull <model> / ollama rm <model>. Un nom simple ajoute seulement au menu.",
    model_cmd_hint: "ollama list / ollama pull <model> / ollama rm <model> ...",
    deep_think: "Réflexion profonde",
    on: "On",
    off: "Off",
    composer_placeholder: "Saisissez un prompt. Entrée = envoyer, Shift+Entrée = nouvelle ligne",
    send: "Envoyer",
    remove: "Retirer",
    streaming_failed: "Échec de la requête, réessayez.",
    thinking: "Réflexion en cours...",
    thinking_done: "Réflexion terminée",
    generating: "Génération de la réponse...",
    answer_format_error: "Format de réponse invalide. Régénérez.",
    expand: "Déplier",
    collapse: "Replier",
    retry: "Réessayer",
    copy: "Copier",
    regenerate: "Régénérer",
    new_chat: "Nouveau chat",
    search_chat: "Rechercher l'historique",
    clear_current: "Vider courant",
    export: "Exporter",
    no_chat_match: "Aucun historique correspondant.",
    pinned: "Épinglé",
    pin: "Épingler",
    unpin: "Désépingler",
    rename: "Renommer",
    delete: "Supprimer",
    empty_session: "Session vide",
    question_count: "{count} questions",
    orders_title: "Page de tâches",
    orders_subtitle: "Pour les mineurs: voir les tâches en attente, les prendre, et suivre l'historique.",
    my_miner_identity: "Identité mineur",
    worker_placeholder: "Saisir le nom du mineur",
    worker_bound: "Lié automatiquement au miner_name local; modifiez ce champ pour forcer.",
    worker_unbound: "Ce nom sera écrit dans le champ miner_name.",
    claim_config: "Configuration",
    auto_claim_on: "Auto-prise active",
    auto_claim_off: "Activer auto-prise",
    status_refreshing: "Actualisation des tâches...",
    status_synced: "Liste synchronisée.",
    status_load_failed: "Échec du chargement: {error}",
    success_rate: "Taux de succès",
    avg_first_token: "First token moyen",
    fail_top: "Top échec",
    mine_total: "Mes tâches",
    mine_active: "En traitement",
    mine_done_failed: "Terminé / Échec",
    local_models_ready: "Modèles locaux prêts",
    tier_27b: "Palier 27b",
    tier_9b: "Palier 9b",
    tier_2b: "Palier 2b",
    rec_16gb: "Recommandé 16GB+ VRAM",
    rec_6gb: "Recommandé 6GB+ VRAM",
    rec_4gb: "Recommandé 4GB+ VRAM",
    view_eligible: "Éligibles",
    view_all: "Toutes",
    view_mine: "Mes tâches",
    visible_mine_title: "Mes tâches prises",
    visible_all_title: "Toutes les tâches",
    visible_eligible_title: "Tâches éligibles",
    visible_mine_desc: "Tâches récentes de l'identité mineur actuelle.",
    visible_all_desc: "Toutes les tâches pending de source frontend.",
    visible_eligible_desc: "Seulement les tâches compatibles avec cette machine.",
    empty_mine: "Vous n'avez encore pris aucune tâche.",
    empty_orders: "Aucune tâche ne correspond au filtre.",
    claim_done: "Pris",
    claim_ing: "Prise...",
    claim_action: "Prendre",
    over_capability: "Hors capacité",
    model_missing: "Non installé",
    order_requester: "Demandeur",
    installed: "Installé localement",
    not_installed: "Non installé localement",
    unknown: "unknown",
    created_at: "Créé",
    claimed_at: "Pris",
    completed_at: "Terminé",
    footer_orders: "Page tâches pour le minage. Revenez au chat pour converser.",
    app_title: "HEPH",
    app_subtitle: "Interface minimale, détails à la demande.",
    rename_chat_prompt: "Renommer le chat",
    delete_chat_confirm: "Supprimer le chat « {title} » ? Action irréversible.",
    clear_chat_confirm: "Vider la conversation courante ? Action irréversible.",
    model_missing_notice: "Aucun modèle disponible. Ajoutez ou installez d'abord un modèle.",
    mode_local: "Local",
    mode_auto: "Auto",
    mode_remote: "Distant",
    orders_word: "tâches",
    eligible_suffix: "éligibles",
    order_status_pending: "En attente",
    order_status_claimed: "Pris",
    order_status_processing: "En traitement",
    order_status_completed: "Terminé",
    order_status_failed: "Échec",
    order_status_cancelled: "Annulé",
    stop_generating: "Arrêter la génération",
    uploaded_image_alt: "image téléversée",
    reasoning_std: "Standard",
    reasoning_deep: "Profond",
    cap_known_auto: "Config mineur actuelle: {vram}GB, prise par défaut jusqu'à {label}.",
    cap_unknown_auto: "Configuration mineur inconnue; Auto ne limite pas encore le palier de modèle.",
    cap_manual: "Limite manuelle fixée à <= {pref}.",
    cap_exceed_msg: "Dépasse la capacité actuelle du mineur, prise impossible pour l'instant.",
    model_not_installed_msg: "Modèle non installé localement, prise impossible pour l'instant.",
    current_miner: "Mineur actuel",
    claimed_at_label: "Pris à",
    completed_at_label: "Terminé à",
    login_title: "Connexion",
    login_subtitle: "Connectez-vous avec Google, GitHub, e-mail ou téléphone.",
    login_google: "Continuer avec Google",
    login_github: "Continuer avec GitHub",
    login_email_label: "E-mail",
    login_phone_label: "Téléphone",
    login_send_email: "Envoyer code e-mail",
    login_send_phone: "Envoyer code SMS",
    login_otp_label: "Code OTP",
    login_verify_email: "Vérifier code e-mail",
    login_verify_phone: "Vérifier code SMS",
    login_sign_out: "Se déconnecter",
    login_error_prefix: "Échec de connexion: {error}",
    login_missing_cfg: "Configuration Supabase manquante. Définissez window.__SUPABASE_URL et window.__SUPABASE_ANON_KEY.",
  },
};

function translateText(lang, key, vars = {}) {
  const table = I18N[lang] || I18N.en;
  const fallback = I18N.en || {};
  let text = table[key] ?? fallback[key] ?? key;
  for (const [k, v] of Object.entries(vars || {})) {
    text = text.replace(new RegExp(`\\{${k}\\}`, "g"), String(v));
  }
  return text;
}

function isAllowedModel(name) {
  return typeof name === "string" && name.trim().length > 0;
}

function parseOllamaList(output) {
  return String(output || "")
    .split(/\r?\n/)
    .slice(1)
    .map((line) => line.trim().split(/\s+/)[0])
    .filter((name) => name && name !== "NAME" && isAllowedModel(name));
}

function parseAllowedOllamaInput(value) {
  const text = String(value || "").trim();
  const match = text.match(/^ollama\s+(list|pull|rm)(?:\s+(.+))?$/i);
  if (!match) return null;
  const action = match[1].toLowerCase();
  const model = String(match[2] || "").trim();
  if (action === "list" && !model) return { action, model: "", command: "ollama list" };
  if ((action === "pull" || action === "rm") && model) {
    return { action, model, command: `ollama ${action} ${model}` };
  }
  return null;
}

function formatElapsed(ms) {
  return `${(Math.max(ms, 0) / 1000).toFixed(1)}s`;
}

function formatCredits(value, unit = "credits") {
  return `${Number(value).toFixed(2)} ${unit}`;
}

function formatAgo(seconds) {
  if (seconds == null) return "unknown";
  if (seconds < 1) return "just now";
  if (seconds < 60) return `${Math.round(seconds)}s ago`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m ago`;
  return `${Math.round(seconds / 3600)}h ago`;
}

function statusMark(status) {
  if (status === "completed") return "✓";
  if (status === "cancelled") return "■";
  if (status === "failed") return "!";
  return "…";
}

function statusDotClass(status) {
  if (status === "cancelled") return "bg-amber-300";
  if (status === "failed") return "bg-red-400";
  if (status === "completed") return "bg-emerald-400";
  return "bg-zinc-300 status-dot-pulse";
}

function normalizeMode(mode) {
  return String(mode || "Auto").toLowerCase();
}

function getUserId() {
  try {
    const existing = localStorage.getItem(USER_KEY);
    if (existing) return existing;
    const next = crypto.randomUUID();
    localStorage.setItem(USER_KEY, next);
    return next;
  } catch {
    return crypto.randomUUID();
  }
}

function getInitialPage() {
  const hash = String(window.location.hash || "").replace(/^#/, "").trim().toLowerCase();
  if (hash === "orders") return "orders";
  return "chat";
}

function clearSupabaseStoredSessionTokens() {
  try {
    const keys = [];
    for (let index = 0; index < localStorage.length; index += 1) {
      const key = String(localStorage.key(index) || "");
      if (!key) continue;
      if (/^sb-.*-auth-token$/i.test(key) || key.includes("supabase.auth.token")) {
        keys.push(key);
      }
    }
    keys.forEach((key) => {
      try {
        localStorage.removeItem(key);
      } catch {}
    });
  } catch {}
  try {
    if (window.sessionStorage) {
      const keys = [];
      for (let index = 0; index < window.sessionStorage.length; index += 1) {
        const key = String(window.sessionStorage.key(index) || "");
        if (!key) continue;
        if (/^sb-.*-auth-token$/i.test(key) || key.includes("supabase.auth.token")) {
          keys.push(key);
        }
      }
      keys.forEach((key) => {
        try {
          window.sessionStorage.removeItem(key);
        } catch {}
      });
    }
  } catch {}
}

function getWorkerName() {
  try {
    const existing = localStorage.getItem(WORKER_KEY);
    if (existing) return existing;
    const next = `user-${crypto.randomUUID().slice(0, 8)}`;
    localStorage.setItem(WORKER_KEY, next);
    return next;
  } catch {
    return `user-${crypto.randomUUID().slice(0, 8)}`;
  }
}

function getAutoClaimEnabled() {
  try {
    return localStorage.getItem(AUTO_CLAIM_KEY) === "1";
  } catch {
    return false;
  }
}

function getClaimCapabilityPreference() {
  try {
    const saved = localStorage.getItem(CLAIM_CAPABILITY_KEY);
    return saved || "auto";
  } catch {
    return "auto";
  }
}

function createSupabaseClient() {
  if (!SUPABASE_URL || !SUPABASE_ANON_KEY) return null;
  if (!window.supabase || typeof window.supabase.createClient !== "function") return null;
  try {
    return window.supabase.createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
      auth: {
        persistSession: false,
        autoRefreshToken: false,
        detectSessionInUrl: false,
      },
    });
  } catch {
    return null;
  }
}

function getInitialTheme() {
  try {
    const saved = localStorage.getItem(THEME_KEY);
    return saved === "light" ? "light" : "dark";
  } catch {
    return "dark";
  }
}

function getInitialLanguage() {
  try {
    const saved = localStorage.getItem(LANG_KEY);
    if (saved && Object.prototype.hasOwnProperty.call(I18N, saved)) return saved;
  } catch {}
  try {
    const params = new URLSearchParams(window.location.search || "");
    const queryLang = String(params.get("appLang") || "").trim().toLowerCase();
    if (queryLang && Object.prototype.hasOwnProperty.call(I18N, queryLang)) {
      return queryLang;
    }
  } catch {}
  const installerLang = String(window.__INSTALLER_LANG || "").trim().toLowerCase();
  if (installerLang && Object.prototype.hasOwnProperty.call(I18N, installerLang)) {
    return installerLang;
  }
  return "en";
}

function getModelCapabilityScore(modelName) {
  const text = String(modelName || "").toLowerCase();
  const match = text.match(/(\d+(?:\.\d+)?)b/);
  if (!match) return 2;
  const size = Number(match[1]);
  if (size >= 20) return 3;
  if (size >= 6) return 2;
  return 1;
}

function getCapabilityScoreFromPreference(preference, profile) {
  if (preference === "2b") return 1;
  if (preference === "9b") return 2;
  if (preference === "27b") return 3;
  return Number(profile?.capability_score || 0) || null;
}

function getRecommendedCapacity(modelName, t = null) {
  const tr = typeof t === "function" ? t : (key) => ({
    tier_27b: "27b tier",
    tier_9b: "9b tier",
    tier_2b: "2b tier",
    rec_16gb: "Recommended 16GB+ VRAM",
    rec_6gb: "Recommended 6GB+ VRAM",
    rec_4gb: "Recommended 4GB+ VRAM",
  }[key] || key);
  const score = getModelCapabilityScore(modelName);
  if (score >= 3) {
    return { tier: tr("tier_27b"), vramLabel: tr("rec_16gb") };
  }
  if (score >= 2) {
    return { tier: tr("tier_9b"), vramLabel: tr("rec_6gb") };
  }
  return { tier: tr("tier_2b"), vramLabel: tr("rec_4gb") };
}

function getLocalNoTokenTimeoutMs(modelName) {
  const score = getModelCapabilityScore(modelName);
  if (score >= 3) return 120 * 1000;
  if (score >= 2) return 90 * 1000;
  return LOCAL_NO_TOKEN_TIMEOUT_MS;
}

function formatOrderRequesterId(userId) {
  const raw = String(userId || "").trim();
  if (!raw) return "anonymous";
  if (raw.length <= 16) return raw;
  return `${raw.slice(0, 8)}...${raw.slice(-4)}`;
}

const API_ERROR_TEXT = {
  en: {
    unauthorized: "Authentication expired. Refresh and retry.",
    forbidden: "You are not allowed to do this.",
    bad_request: "Invalid request parameters.",
    not_found: "Target resource not found.",
    conflict: "Task state changed. Refresh and retry.",
    rate_limited: "Too many requests. Try again later.",
    service_unavailable: "Service unavailable. Check gateway configuration.",
    timeout: "Request timed out. Try again later.",
    local_model_missing: "Local model is not installed.",
    insufficient_credits: "Insufficient credit.",
    miner_ineligible: "Current miner does not meet task requirements.",
    no_pending_task: "No pending task available.",
    claim_race_lost: "Task has been claimed by another miner.",
    internal_error: "Internal server error. Please retry.",
  },
  zh: {
    unauthorized: "认证失效，请刷新页面后重试。",
    forbidden: "你没有权限执行这个操作。",
    bad_request: "请求参数不正确。",
    not_found: "目标资源不存在。",
    conflict: "任务状态已变化，请刷新后重试。",
    rate_limited: "请求过于频繁，请稍后再试。",
    service_unavailable: "服务暂时不可用，请检查网关配置。",
    timeout: "请求超时，请稍后重试。",
    local_model_missing: "本机未安装该模型，请先安装后再试。",
    insufficient_credits: "Credit 不足，请稍后再试。",
    miner_ineligible: "当前矿工配置不满足任务要求。",
    no_pending_task: "暂无可接任务。",
    claim_race_lost: "任务已被其他矿工接走。",
    internal_error: "服务内部错误，请重试。",
  },
  ja: {
    unauthorized: "認証が失効しました。更新して再試行してください。",
    forbidden: "この操作の権限がありません。",
    bad_request: "リクエストパラメータが不正です。",
    not_found: "対象リソースが見つかりません。",
    conflict: "タスク状態が変わりました。更新して再試行してください。",
    rate_limited: "リクエストが多すぎます。しばらく待ってください。",
    service_unavailable: "サービス利用不可。ゲートウェイ設定を確認してください。",
    timeout: "タイムアウトしました。再試行してください。",
    local_model_missing: "ローカルモデルが未インストールです。",
    insufficient_credits: "クレジット不足です。",
    miner_ineligible: "現在のマイナー構成では実行できません。",
    no_pending_task: "待機中タスクはありません。",
    claim_race_lost: "他のマイナーに先に受注されました。",
    internal_error: "サーバー内部エラー。再試行してください。",
  },
  fr: {
    unauthorized: "Session expirée. Actualisez puis réessayez.",
    forbidden: "Vous n'avez pas l'autorisation.",
    bad_request: "Paramètres de requête invalides.",
    not_found: "Ressource introuvable.",
    conflict: "État de tâche modifié. Actualisez puis réessayez.",
    rate_limited: "Trop de requêtes. Réessayez plus tard.",
    service_unavailable: "Service indisponible. Vérifiez la configuration.",
    timeout: "Délai dépassé. Réessayez.",
    local_model_missing: "Le modèle local n'est pas installé.",
    insufficient_credits: "Crédit insuffisant.",
    miner_ineligible: "Le mineur actuel ne répond pas aux exigences.",
    no_pending_task: "Aucune tâche en attente.",
    claim_race_lost: "Tâche déjà prise par un autre mineur.",
    internal_error: "Erreur interne du serveur. Réessayez.",
  },
};

function getApiErrorMessage(payload, fallback = "Request failed. Please retry.", lang = "en") {
  const code = String(payload?.code || "").trim();
  const rawMessage = String(payload?.message || "").trim();
  const table = API_ERROR_TEXT[lang] || API_ERROR_TEXT.en;
  if (code && table[code]) return table[code];
  return rawMessage || fallback;
}

function isNotFoundApiPayload(payload) {
  return String(payload?.code || "").trim() === "not_found";
}

function getMessageAnswerText(message) {
  if (!message || message.role !== "assistant") return "";
  const parsed = parseReasoningContent(message.content || "");
  return (parsed.answer || message.content || "").trim();
}

function downloadTextFile(filename, text) {
  const blob = new Blob([text], { type: "text/plain;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}

function summarizeConversationTitle(messages) {
  const firstUser = (messages || []).find((item) => item?.role === "user" && String(item.content || "").trim());
  if (!firstUser) return "New chat";
  const normalized = String(firstUser.content || "")
    .replace(/\[图片:[^\]]+\]/g, "")
    .replace(/\s+/g, " ")
    .trim();
  if (!normalized) return "New chat";
  return normalized.length > 20 ? `${normalized.slice(0, 20)}…` : normalized;
}

function createChatSession(title = "New chat") {
  return {
    id: crypto.randomUUID(),
    title,
    updatedAt: new Date().toISOString(),
    pinned: false,
    messages: [],
  };
}

function isInvalidAssistantMessageForRestore(message) {
  if (!message || message.role !== "assistant") return false;
  if (String(message.status || "").toLowerCase() !== "completed") return true;
  const parsed = parseReasoningContent(message.content || "");
  const content = String(parsed.answer || message.content || "")
    .replace(/<\/?(?:think|answer)>/gi, "")
    .trim();
  const normalized = content.replace(/\\n/g, "\n").trim().toLowerCase();
  if (
    !content ||
    normalized === "your answer here" ||
    normalized === "write the final answer only." ||
    normalized === "final answer only." ||
    (/^[`'".,;:!?()\[\]{}<>\-_/\\|&+\s]+$/.test(content) && !/[\u4e00-\u9fffA-Za-z0-9]/.test(content))
  ) {
    return true;
  }
  if (looksLikeReasoningLeak(content) && !parsed.answer) {
    return true;
  }
  return false;
}

function sanitizeLoadedMessages(messages) {
  return Array.isArray(messages)
    ? messages.filter((item) => {
        if (!item || !["user", "assistant"].includes(item.role)) return false;
        if (item.role === "assistant" && isInvalidAssistantMessageForRestore(item)) return false;
        return Boolean(String(item.content || "").trim());
      })
    : [];
}

function getInitialChatState() {
  try {
    const raw = localStorage.getItem(CHAT_SESSIONS_KEY);
    const current = localStorage.getItem(CURRENT_CHAT_KEY);
    const parsed = raw ? JSON.parse(raw) : [];
    const sessions = Array.isArray(parsed)
      ? parsed
          .filter((item) => item && item.id && Array.isArray(item.messages))
          .map((item) => ({
            ...item,
            pinned: Boolean(item.pinned),
            messages: sanitizeLoadedMessages(item.messages),
          }))
      : [];
    if (sessions.length > 0) {
      const active = sessions.find((item) => item.id === current) || sessions[0];
      return {
        sessions,
        currentChatId: active.id,
        messages: active.messages,
      };
    }
  } catch {}
  const next = createChatSession();
  return {
    sessions: [next],
    currentChatId: next.id,
    messages: next.messages,
  };
}

function sortChatSessions(sessions) {
  return [...sessions].sort((a, b) => {
    const pinnedDiff = Number(Boolean(b?.pinned)) - Number(Boolean(a?.pinned));
    if (pinnedDiff !== 0) return pinnedDiff;
    const aTime = Date.parse(a?.updatedAt || "") || 0;
    const bTime = Date.parse(b?.updatedAt || "") || 0;
    return bTime - aTime;
  });
}

function parseMs(value) {
  const ms = Date.parse(value || "");
  return Number.isNaN(ms) ? null : ms;
}

function diffMs(start, end) {
  const startMs = parseMs(start);
  const endMs = parseMs(end);
  if (startMs === null || endMs === null) return 0;
  return Math.max(0, endMs - startMs);
}

function buildConversationHistory(messages) {
  return messages
    .filter((item) => {
      if (!item || !["user", "assistant"].includes(item.role)) return false;
      if (item.role === "assistant") {
        if (String(item.status || "").toLowerCase() !== "completed") return false;
      }
      return Boolean(String(item.content || "").trim());
    })
    .slice(-MAX_CONTEXT_MESSAGES)
    .map((item) => {
      const parsed = item.role === "assistant" ? parseReasoningContent(item.content) : null;
      if (item.role === "assistant" && looksLikeReasoningLeak(item.content) && !parsed?.answer) {
        return null;
      }
      const rawContent =
        item.role === "assistant"
          ? parsed?.answer || (looksLikeReasoningLeak(item.content) ? extractFallbackAnswerFromLeak(item.content) : item.content)
          : item.content;
      let content = String(rawContent || "")
        .replace(/<\/?(?:think|answer)>/gi, "")
        .trim();
      if (item.role === "assistant") {
        const normalized = content.replace(/\\n/g, "\n").trim().toLowerCase();
        if (
          !content ||
          normalized === "your answer here" ||
          normalized === "write the final answer only." ||
          normalized === "final answer only." ||
          (/^[`'".,;:!?()\[\]{}<>\-_/\\|&+\s]+$/.test(content) && !/[\u4e00-\u9fffA-Za-z0-9]/.test(content))
        ) {
          return null;
        }
      }
      if (content.length > MAX_CONTEXT_CONTENT_CHARS) {
        content = `${content.slice(0, MAX_CONTEXT_CONTENT_CHARS).trim()}\n...(truncated)`;
      }
      return {
        role: item.role,
        content,
      };
    })
    .filter((item) => item && Boolean(item.content));
}

function resolveExec(mode, minerName) {
  if (minerName) return "Remote";
  const normalized = normalizeMode(mode);
  if (normalized === "local") return "Local";
  if (normalized === "remote") return "Remote";
  if (String(mode || "").toLowerCase() === "routing") return "Routing";
  return "Auto";
}

function parseReasoningContent(text) {
  const raw = String(text || "");
  if (!raw.trim()) return { reasoning: "", answer: "" };
  const normalized = raw.replace(/\r\n/g, "\n");
  const OPEN_THINK = "<think>";
  const CLOSE_THINK = "</think>";
  const OPEN_ANSWER = "<answer>";
  const CLOSE_ANSWER = "</answer>";
  let cursor = 0;
  let mode = "unknown";
  let reasoning = "";
  let answer = "";
  let sawProtocolTag = false;

  while (cursor < normalized.length) {
    if (mode === "think") {
      const closeIndex = normalized.indexOf(CLOSE_THINK, cursor);
      if (closeIndex === -1) {
        reasoning += normalized.slice(cursor);
        break;
      }
      reasoning += normalized.slice(cursor, closeIndex);
      cursor = closeIndex + CLOSE_THINK.length;
      mode = "unknown";
      continue;
    }

    if (mode === "answer") {
      const closeIndex = normalized.indexOf(CLOSE_ANSWER, cursor);
      if (closeIndex === -1) {
        answer += normalized.slice(cursor);
        break;
      }
      answer += normalized.slice(cursor, closeIndex);
      cursor = closeIndex + CLOSE_ANSWER.length;
      mode = "unknown";
      continue;
    }

    const nextThink = normalized.indexOf(OPEN_THINK, cursor);
    const nextAnswer = normalized.indexOf(OPEN_ANSWER, cursor);
    const nextCandidates = [nextThink, nextAnswer].filter((value) => value >= 0);

    if (nextCandidates.length === 0) {
      const remainder = normalized.slice(cursor);
      if (!sawProtocolTag) {
        answer += remainder;
      } else if (remainder.trim()) {
        answer += remainder;
      }
      break;
    }

    const nextIndex = Math.min(...nextCandidates);
    if (!sawProtocolTag && nextIndex > cursor) {
      answer += normalized.slice(cursor, nextIndex);
    }

    if (nextIndex === nextThink) {
      sawProtocolTag = true;
      cursor = nextThink + OPEN_THINK.length;
      mode = "think";
      continue;
    }

    sawProtocolTag = true;
    cursor = nextAnswer + OPEN_ANSWER.length;
    mode = "answer";
  }

  const stripProtocolTags = (value) =>
    String(value || "")
      .replace(/<\/?(?:think|answer)>/gi, "")
      .trim();

  return {
    reasoning: stripProtocolTags(reasoning),
    answer: stripProtocolTags(answer),
  };
}

function looksLikeReasoningLeak(text) {
  const sample = String(text || "").trim().slice(0, 280).toLowerCase();
  if (!sample) return false;
  return /^(okay|ok,|first,|let me|i need to|i should|we need to|hmm|wait,|the user|user asked|thinking process|analysis:|current user request|continue in the same language as before|the previous responses|the assistant has been replying)/i.test(sample);
}

function extractFallbackAnswerFromLeak(text) {
  const raw = String(text || "").trim();
  if (!raw) return "";
  const answerMatch = raw.match(/<answer>([\s\S]*?)(?:<\/answer>|$)/i);
  if (answerMatch?.[1]?.trim()) return answerMatch[1].trim();
  const lines = raw
    .split(/\n+/)
    .map((line) => line.trim())
    .filter(Boolean)
    .filter((line) => !looksLikeReasoningLeak(line))
    .filter((line) => !/^(the user|current user|possible response|the key points|the instruction|wait,|but the instruction)/i.test(line));
  const shortChineseLine = [...lines].reverse().find((line) => /[\u4e00-\u9fff]/.test(line) && line.length <= 80);
  if (shortChineseLine) return shortChineseLine;
  const shortLine = [...lines].reverse().find((line) => line.length <= 120);
  return shortLine || "";
}

function readFileAsDataUrl(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(new Error("Image read failed"));
    reader.readAsDataURL(file);
  });
}

function PillDropdown({
  value,
  options,
  onChange,
  disabled = false,
  buttonClassName = "",
  menuClassName = "",
}) {
  const [open, setOpen] = useState(false);
  const rootRef = useRef(null);
  const safeOptions = Array.isArray(options) ? options : [];
  const selected = safeOptions.find((item) => item.value === value) || safeOptions[0] || null;

  useEffect(() => {
    const onDown = (event) => {
      if (!rootRef.current) return;
      if (!rootRef.current.contains(event.target)) {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", onDown);
    return () => document.removeEventListener("mousedown", onDown);
  }, []);

  useEffect(() => {
    if (disabled) setOpen(false);
  }, [disabled]);

  return (
    <div ref={rootRef} className="relative">
      <button
        type="button"
        disabled={disabled}
        onClick={() => setOpen((prev) => !prev)}
        className={`app-pill-dropdown ${buttonClassName}`}
      >
        <span className="truncate">{selected?.label || ""}</span>
        <span className={`app-pill-chevron ${open ? "open" : ""}`} aria-hidden="true">⌄</span>
      </button>
      {open && !disabled && safeOptions.length > 0 && (
        <div className={`app-dropdown-menu ${menuClassName}`}>
          {safeOptions.map((item) => (
            <button
              key={item.value}
              type="button"
              onClick={() => {
                onChange(item.value);
                setOpen(false);
              }}
              className={`app-dropdown-item ${item.value === selected?.value ? "active" : ""}`}
            >
              {item.label}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

function TopBar({
  model,
  setModel,
  models,
  mode,
  setMode,
  credits,
  addModel,
  page,
  setPage,
  theme,
  setTheme,
  lang,
  setLang,
  t,
  runOllamaCommand,
  modelCommandBusy,
  modelCommandStatus,
  installOrRepairOllama,
  ollamaInstallBusy,
  ollamaRuntime,
  onSignOut,
}) {
  const [draft, setDraft] = useState("");
  const currentDraft = draft.trim();
  const parsedCommand = parseAllowedOllamaInput(currentDraft);
  const executeDraftCommand = async () => {
    if (modelCommandBusy) return;
    if (parsedCommand) {
      await runOllamaCommand(parsedCommand.action, parsedCommand.model, parsedCommand.command);
      setDraft("");
      return;
    }
    if (currentDraft) {
      addModel(currentDraft);
      setDraft("");
    }
  };
  const modeLabel = (item) => {
    const key = `mode_${String(item || "").toLowerCase()}`;
    return t(key);
  };
  const modelDropdownOptions = models.length
    ? models.map((item) => ({ value: item, label: item }))
    : [{ value: "", label: t("no_local_models") }];
  const langDropdownOptions = LANGUAGE_OPTIONS.map((item) => ({ value: item.key, label: item.label }));

  return (
    <header className="border-b border-line px-4 py-3 sm:px-6">
      <div className="flex flex-wrap items-center gap-2">
        <div className="flex flex-wrap items-center gap-2">
          <PillDropdown
            value={model}
            options={modelDropdownOptions}
            disabled={models.length === 0}
            onChange={(nextValue) => setModel(nextValue)}
            buttonClassName="h-9 min-w-[180px] rounded-full border border-line bg-panel px-3 text-left text-sm text-zinc-200"
            menuClassName="w-[280px] max-h-[320px] overflow-y-auto"
          />
          <input
            value={draft}
            onChange={(event) => setDraft(event.target.value)}
            onKeyDown={(event) => {
              if (event.key !== "Enter") return;
              event.preventDefault();
              executeDraftCommand();
            }}
            placeholder={t("add_model_placeholder")}
            className="h-9 w-28 rounded-full border border-line bg-panel px-3 text-xs text-zinc-300 placeholder:text-zinc-500 outline-none sm:w-36"
          />
          <button
            type="button"
            onClick={() => {
              executeDraftCommand();
            }}
            className="h-9 rounded-full border border-line bg-panel px-3 text-xs text-zinc-300 transition hover:text-zinc-100"
            title={t("model_cmd_title")}
          >
            +
          </button>
          <button
            type="button"
            onClick={installOrRepairOllama}
            disabled={Boolean(ollamaInstallBusy)}
            className="h-9 rounded-full border border-line bg-panel px-3 text-xs text-zinc-300 transition hover:text-zinc-100 disabled:cursor-not-allowed disabled:opacity-50"
            title={t("ollama_install_repair")}
          >
            {t("ollama_install_repair")}
          </button>
          {modelCommandStatus && (
            <span className="max-w-[220px] truncate text-xs text-zinc-500" title={modelCommandStatus}>
              {modelCommandStatus}
            </span>
          )}
          {!modelCommandStatus && ollamaRuntime?.source === "bundled" && (
            <span className="max-w-[220px] truncate text-xs text-zinc-500" title={t("ollama_runtime_light")}>
              {t("ollama_runtime_light")}
            </span>
          )}
          {!modelCommandStatus && ollamaRuntime?.source === "missing" && (
            <span className="max-w-[220px] truncate text-xs text-zinc-500" title={t("ollama_runtime_missing")}>
              {t("ollama_runtime_missing")}
            </span>
          )}
        </div>

        {page === "chat" && (
          <div className="flex items-center rounded-full border border-line bg-panel p-1">
            {MODE_OPTIONS.map((item) => (
              <button
                key={item}
                type="button"
                onClick={() => setMode(item)}
                className={`h-7 rounded-full px-3 text-xs transition ${
                  mode === item
                    ? "bg-zinc-200 text-zinc-900"
                    : "text-zinc-400 hover:text-zinc-100"
                }`}
              >
                {modeLabel(item)}
              </button>
            ))}
          </div>
        )}

        <div className="flex items-center rounded-full border border-line bg-panel p-1">
          {PAGE_OPTIONS.map((item) => (
            <button
              key={item.key}
              type="button"
              onClick={() => setPage(item.key)}
              className={`h-7 rounded-full px-3 text-xs transition ${
                page === item.key
                  ? "bg-zinc-200 text-zinc-900"
                  : "text-zinc-400 hover:text-zinc-100"
              }`}
            >
              {t(`page_${item.key}`)}
            </button>
          ))}
        </div>

        <div className="ml-auto flex items-center gap-2">
          <PillDropdown
            value={lang}
            options={langDropdownOptions}
            onChange={(nextValue) => setLang(nextValue)}
            buttonClassName="h-7 min-w-[96px] rounded-full border border-line bg-panel px-3 text-left text-xs text-zinc-300"
            menuClassName="w-[148px] max-h-[220px] overflow-y-auto"
          />
          <div className="flex items-center rounded-full border border-line bg-panel p-1">
            <button
              type="button"
              onClick={() => setTheme("light")}
              className={`h-7 rounded-full px-3 text-xs transition ${
                theme === "light"
                  ? "bg-zinc-200 text-zinc-900"
                  : "text-zinc-400 hover:text-zinc-100"
              }`}
            >
              {t("theme_light")}
            </button>
            <button
              type="button"
              onClick={() => setTheme("dark")}
              className={`h-7 rounded-full px-3 text-xs transition ${
                theme === "dark"
                  ? "bg-zinc-200 text-zinc-900"
                  : "text-zinc-400 hover:text-zinc-100"
              }`}
            >
              {t("theme_dark")}
            </button>
          </div>
          <div className="rounded-full border border-line bg-panel px-3 py-2 text-xs text-zinc-300">
          {Number.isFinite(Number(credits)) ? Number(credits).toFixed(2) : "0.00"} {t("credit")}
          </div>
          <button
            type="button"
            onClick={onSignOut}
            className="h-9 rounded-full border border-line bg-panel px-3 text-xs text-zinc-300 transition hover:text-zinc-100"
          >
            {t("login_sign_out")}
          </button>
        </div>
      </div>
    </header>
  );
}

function DetailCard({ meta, status }) {
  return (
    <div className="mt-2 grid w-full max-w-2xl grid-cols-1 gap-2 rounded-2xl border border-line bg-panel/60 p-3 text-xs text-zinc-300 sm:grid-cols-2">
      <div><span className="text-zinc-500">Model:</span> {meta.model}</div>
      <div><span className="text-zinc-500">Source:</span> {meta.exec}</div>
      <div><span className="text-zinc-500">Reasoning:</span> {meta.deepThink ? "Deep" : "Standard"}</div>
      <div><span className="text-zinc-500">Status:</span> {status}</div>
      <div><span className="text-zinc-500">Elapsed:</span> {formatElapsed(meta.elapsedMs)}</div>
      <div><span className="text-zinc-500">First token:</span> {formatElapsed(meta.firstTokenMs)}</div>
      <div><span className="text-zinc-500">Cost:</span> {formatCredits(meta.credits)}</div>
      <div className="sm:col-span-2"><span className="text-zinc-500">Task ID:</span> {meta.taskId}</div>
      <div className="sm:col-span-2"><span className="text-zinc-500">Node:</span> {meta.node}</div>
    </div>
  );
}

function ChatMessage({ msg, onToggle, onRetry, onRegenerate, onCopy, t }) {
  const isAssistant = msg.role === "assistant";
  const parsed = isAssistant ? parseReasoningContent(msg.content) : null;
  const reasoning = parsed?.reasoning || "";
  const answer = parsed?.answer || "";
  const hasAnswer = Boolean(answer.trim());
  const hasProtocolTags = /<\/?(?:think|answer)>/i.test(String(msg.content || ""));
  const hasProtocolContent = hasProtocolTags && Boolean((reasoning || answer).trim());
  const hideNormalReasoningLeak =
    isAssistant &&
    !msg.meta?.deepThink &&
    !hasProtocolTags &&
    looksLikeReasoningLeak(msg.content);
  const shouldShowAnswer = !isAssistant || !reasoning || hasAnswer || msg.status === "failed";
  const reasoningSummary =
    msg.status === "processing"
      ? t("thinking")
      : `${t("thinking_done")}${reasoning ? ` · ${Math.max(1, reasoning.split(/\s+/).filter(Boolean).length)} tokens` : ""}`;
  const displayAnswer =
    hideNormalReasoningLeak
      ? msg.status === "processing"
        ? t("generating")
        : extractFallbackAnswerFromLeak(msg.content) || t("answer_format_error")
      : isAssistant && hasProtocolContent
        ? answer
        : msg.content;
  const [animatedAnswer, setAnimatedAnswer] = useState(displayAnswer);

  useEffect(() => {
    const full = String(displayAnswer || "");
    const shouldAnimate =
      isAssistant &&
      shouldShowAnswer &&
      msg.status === "completed" &&
      !msg.meta?.hadStreamDelta &&
      !reasoning &&
      full.length > 0 &&
      full.length <= 240;

    if (!shouldAnimate) {
      setAnimatedAnswer(full);
      return undefined;
    }

    let cancelled = false;
    let timer = null;
    let index = 0;
    const stepSize = Math.max(1, Math.ceil(full.length / 28));
    setAnimatedAnswer("");

    const tick = () => {
      if (cancelled) return;
      index = Math.min(full.length, index + stepSize);
      setAnimatedAnswer(full.slice(0, index));
      if (index < full.length) {
        timer = setTimeout(tick, 18);
      }
    };

    timer = setTimeout(tick, 16);
    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
    };
  }, [isAssistant, shouldShowAnswer, msg.status, msg.meta?.hadStreamDelta, reasoning, displayAnswer]);

  return (
    <article className={`flex flex-col gap-2 ${msg.role === "user" ? "items-end" : "items-start"}`}>
      {isAssistant && reasoning && (
        <div className="w-full max-w-3xl overflow-hidden rounded-2xl border border-white/8 bg-white/[0.03]">
          <button
            type="button"
            onClick={onToggle("reasoning")}
            className="flex w-full items-center justify-between gap-3 px-4 py-3 text-left transition hover:bg-white/[0.02]"
          >
            <div className="flex items-center gap-2 text-xs text-zinc-400">
              <span className="rounded-full border border-white/8 bg-white/[0.04] px-2 py-1 text-zinc-300">{t("deep_think")}</span>
              <span>{reasoningSummary}</span>
            </div>
            <span className="text-xs text-zinc-500">{msg.reasoningExpanded ? t("collapse") : t("expand")}</span>
          </button>
          {msg.reasoningExpanded && (
            <div className="whitespace-pre-wrap border-t border-white/6 px-4 py-3 text-sm leading-7 text-zinc-400">
              {reasoning}
            </div>
          )}
        </div>
      )}

      {shouldShowAnswer && (
        <div
          className={`max-w-3xl whitespace-pre-wrap rounded-2xl border px-4 py-3 text-sm leading-7 ${
            msg.role === "user"
              ? "border-zinc-200 bg-zinc-100 text-zinc-900"
              : "border-line bg-panel/50 text-zinc-100"
          }`}
        >
          {msg.imageDataUrl && (
            <img
              src={msg.imageDataUrl}
              alt={msg.imageName || t("uploaded_image_alt")}
              className="mb-3 max-h-56 w-auto max-w-full rounded-2xl border border-black/10 object-cover"
            />
          )}
          {animatedAnswer || (msg.status === "failed" ? t("streaming_failed") : "")}
        </div>
      )}

      {isAssistant && (
        <div className="w-full max-w-3xl">
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={onToggle("detail")}
              className="inline-flex items-center gap-2 text-[11px] text-muted hover:text-zinc-200"
            >
              <span className={`h-1.5 w-1.5 rounded-full ${statusDotClass(msg.status)}`} />
              <span>{statusMark(msg.status)}</span>
              <span>
                {msg.meta.model} · {msg.meta.exec} · {msg.meta.deepThink ? t("reasoning_deep") : t("reasoning_std")} · {formatElapsed(msg.meta.elapsedMs)}
              </span>
            </button>
            {msg.status === "failed" && (
              <button
                type="button"
                onClick={onRetry}
                className="rounded-full border border-line px-2 py-1 text-[11px] text-zinc-400 transition hover:text-zinc-100"
              >
                {t("retry")}
              </button>
            )}
            {msg.status === "completed" && (
              <>
                <button
                  type="button"
                  onClick={onCopy}
                  className="rounded-full border border-line px-2 py-1 text-[11px] text-zinc-400 transition hover:text-zinc-100"
                >
                  {t("copy")}
                </button>
                <button
                  type="button"
                  onClick={onRegenerate}
                  className="rounded-full border border-line px-2 py-1 text-[11px] text-zinc-400 transition hover:text-zinc-100"
                >
                  {t("regenerate")}
                </button>
              </>
            )}
          </div>
          {msg.expanded && <DetailCard meta={msg.meta} status={msg.status} />}
        </div>
      )}
    </article>
  );
}

function ChatSidebar({
  sessions,
  currentChatId,
  onSelectChat,
  onNewChat,
  onRenameChat,
  onDeleteChat,
  onPinChat,
  onClearChat,
  onExportChat,
  disabled,
  t,
}) {
  const [query, setQuery] = useState("");
  const normalizedQuery = query.trim().toLowerCase();
  const filteredSessions = sortChatSessions(sessions).filter((session) => {
    if (!normalizedQuery) return true;
    const title = String(session.title || "").toLowerCase();
    const preview = String(
      (session.messages || [])
        .filter((item) => item?.role === "user")
        .map((item) => item?.content || "")
        .join(" ")
    ).toLowerCase();
    return title.includes(normalizedQuery) || preview.includes(normalizedQuery);
  });

  return (
    <aside className="flex h-full w-full max-w-[280px] flex-col border-r border-line bg-panel/30">
      <div className="border-b border-line p-4">
        <button
          type="button"
          onClick={onNewChat}
          disabled={disabled}
          className="flex h-11 w-full items-center justify-center rounded-2xl border border-line bg-panel text-sm text-zinc-200 transition hover:text-zinc-100 disabled:cursor-not-allowed disabled:opacity-40"
        >
          + {t("new_chat")}
        </button>
        <input
          type="text"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder={t("search_chat")}
          className="mt-3 h-10 w-full rounded-2xl border border-line bg-appbg/60 px-3 text-sm text-zinc-200 outline-none placeholder:text-zinc-500"
        />
        <div className="mt-3 grid grid-cols-2 gap-2">
          <button
            type="button"
            onClick={onClearChat}
            disabled={disabled}
            className="h-9 rounded-2xl border border-line bg-appbg/60 text-xs text-zinc-400 transition hover:text-zinc-100 disabled:cursor-not-allowed disabled:opacity-40"
          >
            {t("clear_current")}
          </button>
          <button
            type="button"
            onClick={onExportChat}
            className="h-9 rounded-2xl border border-line bg-appbg/60 text-xs text-zinc-400 transition hover:text-zinc-100"
          >
            {t("export")}
          </button>
        </div>
      </div>
      <div className="flex-1 space-y-2 overflow-y-auto p-3">
        {filteredSessions.length === 0 ? (
          <div className="rounded-2xl border border-dashed border-line px-3 py-4 text-sm text-zinc-500">
            {t("no_chat_match")}
          </div>
        ) : filteredSessions.map((session) => {
          const isActive = session.id === currentChatId;
          const preview = session.messages.filter((item) => item?.role === "user").length;
          return (
            <div
              key={session.id}
              className={`rounded-2xl border transition ${
                isActive
                  ? "border-zinc-200 bg-zinc-100 text-zinc-900"
                  : "border-line bg-appbg/50 text-zinc-300"
              }`}
            >
              <button
                type="button"
                onClick={() => onSelectChat(session.id)}
                disabled={disabled}
                className="flex w-full flex-col px-3 pb-2 pt-3 text-left disabled:cursor-not-allowed disabled:opacity-50"
              >
                <div className="flex items-center gap-2">
                  {session.pinned && (
                    <span className={`text-[10px] ${isActive ? "text-zinc-700" : "text-amber-300"}`}>{t("pinned")}</span>
                  )}
                  <div className="truncate text-sm font-medium">{session.title || t("new_chat")}</div>
                </div>
                <div className={`mt-1 text-[11px] ${isActive ? "text-zinc-600" : "text-zinc-500"}`}>
                  {preview > 0 ? t("question_count", { count: preview }) : t("empty_session")}
                </div>
              </button>
              <div className="flex gap-1 px-3 pb-3">
                <button
                  type="button"
                  onClick={(event) => {
                    event.stopPropagation();
                    onPinChat(session.id);
                  }}
                  disabled={disabled}
                  className={`rounded-full border px-2 py-1 text-[11px] transition ${
                    isActive
                      ? "border-zinc-300 text-zinc-700"
                      : "border-line text-zinc-400 hover:text-zinc-100"
                  } disabled:cursor-not-allowed disabled:opacity-40`}
                >
                  {session.pinned ? t("unpin") : t("pin")}
                </button>
                <button
                  type="button"
                  onClick={(event) => {
                    event.stopPropagation();
                    onRenameChat(session.id);
                  }}
                  disabled={disabled}
                  className={`rounded-full border px-2 py-1 text-[11px] transition ${
                    isActive
                      ? "border-zinc-300 text-zinc-700"
                      : "border-line text-zinc-400 hover:text-zinc-100"
                  } disabled:cursor-not-allowed disabled:opacity-40`}
                >
                  {t("rename")}
                </button>
                <button
                  type="button"
                  onClick={(event) => {
                    event.stopPropagation();
                    onDeleteChat(session.id);
                  }}
                  disabled={disabled}
                  className={`rounded-full border px-2 py-1 text-[11px] transition ${
                    isActive
                      ? "border-zinc-300 text-zinc-700"
                      : "border-line text-zinc-400 hover:text-red-300"
                  } disabled:cursor-not-allowed disabled:opacity-40`}
                >
                  {t("delete")}
                </button>
              </div>
            </div>
          );
        })}
      </div>
    </aside>
  );
}

function OrdersPage({
  workerName,
  setWorkerName,
  localMinerProfile,
  autoClaimEnabled,
  onToggleAutoClaim,
  claimCapabilityPreference,
  setClaimCapabilityPreference,
  minerProfile,
  effectiveClaimCapabilityScore,
  eligibleOrderCount,
  availableOrders,
  myOrders,
  myOrdersSummary,
  localMineOrders,
  loading,
  error,
  claimLoadingId,
  onClaim,
  installedModels,
  dashboardMetrics,
  t,
}) {
  const [orderView, setOrderView] = useState("eligible");
  const installedModelSet = useMemo(
    () => new Set((installedModels || []).map((item) => String(item || "").trim()).filter(Boolean)),
    [installedModels]
  );

  const renderOrderStatus = (status) => {
    if (status === "pending") return t("order_status_pending");
    if (status === "claimed") return t("order_status_claimed");
    if (status === "processing") return t("order_status_processing");
    if (status === "completed") return t("order_status_completed");
    if (status === "failed") return t("order_status_failed");
    if (status === "cancelled") return t("order_status_cancelled");
    return status || t("unknown");
  };

  const isOrderEligible = (item) => {
    const modelReady = installedModelSet.has(item?.model);
    const capabilityReady = !effectiveClaimCapabilityScore || getModelCapabilityScore(item?.model) <= effectiveClaimCapabilityScore;
    return modelReady && capabilityReady;
  };

  const capabilitySummary =
    claimCapabilityPreference === "auto"
      ? minerProfile?.found
        ? t("cap_known_auto", {
            vram: minerProfile.vram_gb || 0,
            label: minerProfile.capability_label || "9b",
          })
        : t("cap_unknown_auto")
      : t("cap_manual", { pref: claimCapabilityPreference });

  const statusSummary = loading
    ? t("status_refreshing")
    : error
      ? t("status_load_failed", { error })
      : t("status_synced");

  const visibleOrders =
    orderView === "mine"
      ? myOrders
      : orderView === "all"
        ? availableOrders
        : availableOrders.filter((item) => isOrderEligible(item));
  const visibleOrdersLimited = visibleOrders.slice(0, ORDER_VISIBLE_LIMIT);

  const visibleTitle =
    orderView === "mine"
      ? t("visible_mine_title")
      : orderView === "all"
        ? t("visible_all_title")
        : t("visible_eligible_title");

  const visibleDescription =
    orderView === "mine"
      ? t("visible_mine_desc")
      : orderView === "all"
        ? t("visible_all_desc")
        : t("visible_eligible_desc");

  const orderStats = useMemo(() => {
    const mine = myOrders || [];
    const countStatus = (status) =>
      mine.filter((item) => String(item?.status || "").toLowerCase() === status).length;
    const localMine = localMineOrders || [];
    const activeMineSource = localMine.length > 0 ? localMine : mine;
    const completedMine = Number(myOrdersSummary?.completed_count ?? countStatus("completed"));
    const failedMine = Number(myOrdersSummary?.failed_count ?? countStatus("failed"));
    const totalMine = Number(myOrdersSummary?.total_count ?? mine.length);
    const activeMine =
      Number(myOrdersSummary?.active_count ?? activeMineSource.filter((item) =>
        ["claimed", "processing"].includes(String(item?.status || "").toLowerCase())
      ).length);
    const installedReady = installedModelSet.size;
    const terminalCount = completedMine + failedMine;
    const successRate = terminalCount > 0 ? completedMine / terminalCount : 0;
    const firstTokenValues = mine
      .map((item) => Number(item?.first_token_ms || 0))
      .filter((value) => Number.isFinite(value) && value > 0);
    const avgFirstTokenMs = firstTokenValues.length
      ? firstTokenValues.reduce((sum, value) => sum + value, 0) / firstTokenValues.length
      : 0;
    const failCounter = {};
    mine.forEach((item) => {
      if (String(item?.status || "").toLowerCase() !== "failed") return;
      const reason = String(item?.failure_reason || "unknown_failure").trim() || "unknown_failure";
      failCounter[reason] = (failCounter[reason] || 0) + 1;
    });
    const failureTop = Object.entries(failCounter).sort((a, b) => b[1] - a[1])[0] || null;
    return {
      totalMine,
      activeMine,
      completedMine,
      failedMine,
      installedReady,
      successRate,
      avgFirstTokenMs,
      failureTop,
    };
  }, [installedModelSet, localMineOrders, myOrders, myOrdersSummary]);

  const OrderRow = ({ item, action, muted = false }) => (
    <div className={`rounded-2xl border border-line p-4 ${muted ? "bg-panel/10 opacity-60" : "bg-panel/40"}`}>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <div className="text-sm font-medium text-zinc-100">
            {t("order_requester")} {formatOrderRequesterId(item.requester_id_masked)}
          </div>
          <div className="mt-2 flex flex-wrap gap-2 text-xs text-zinc-400">
            <span
              className={`rounded-full border px-2 py-1 ${
                installedModelSet.has(item.model)
                  ? "border-emerald-400/30 text-emerald-200"
                  : "border-amber-400/30 text-amber-200"
              }`}
            >
              {installedModelSet.has(item.model) ? t("installed") : t("not_installed")}
            </span>
            <span className="rounded-full border border-line px-2 py-1">{renderOrderStatus(item.status)}</span>
            <span className="rounded-full border border-line px-2 py-1">{item.model || "auto-model"}</span>
            <span className="rounded-full border border-line px-2 py-1">
              {getRecommendedCapacity(item.model, t).vramLabel}
            </span>
          </div>
          {muted && (
            <div className="mt-2 text-xs text-amber-300">
              {installedModelSet.has(item.model)
                ? t("cap_exceed_msg")
                : t("model_not_installed_msg")}
            </div>
          )}
        </div>
        {action}
      </div>
      <div className="mt-3 text-xs text-zinc-500">
        {t("created_at")} {item.created_at || t("unknown")}
        {item.miner_name ? ` · ${t("current_miner")}：${item.miner_name}` : ""}
        {item.claimed_at ? ` · ${t("claimed_at_label")}：${item.claimed_at}` : ""}
        {item.completed_at ? ` · ${t("completed_at_label")}：${item.completed_at}` : ""}
      </div>
    </div>
  );

  return (
    <div className="mx-auto flex w-full max-w-5xl flex-col gap-5">
      <section className="rounded-3xl border border-line bg-panel/40 p-5">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <div className="text-[11px] uppercase tracking-[0.18em] text-zinc-500">{t("page_orders")}</div>
            <h2 className="mt-2 text-2xl font-medium text-zinc-100">{t("orders_title")}</h2>
            <p className="mt-2 max-w-2xl text-sm leading-6 text-zinc-400">
              {t("orders_subtitle")}
            </p>
          </div>
          <div className="w-full max-w-sm rounded-2xl border border-line bg-appbg/70 p-4">
            <div className="text-xs text-zinc-500">{t("my_miner_identity")}</div>
            <input
              value={workerName}
              onChange={(event) => setWorkerName(event.target.value)}
              placeholder={t("worker_placeholder")}
              className="mt-3 h-11 w-full rounded-xl border border-line bg-panel px-3 text-sm text-zinc-100 outline-none placeholder:text-zinc-500"
            />
            <div className="mt-2 text-xs text-zinc-500">
              {localMinerProfile?.found
                ? t("worker_bound")
                : t("worker_unbound")}
            </div>

            <div className="mt-3 text-xs text-zinc-500">{t("claim_config")}</div>
            <div className="mt-2 flex flex-wrap gap-2">
              {CLAIM_CAPABILITY_OPTIONS.map((item) => (
                <button
                  key={item.key}
                  type="button"
                  onClick={() => setClaimCapabilityPreference(item.key)}
                  className={`rounded-full border px-3 py-2 text-xs transition ${
                    claimCapabilityPreference === item.key
                      ? "border-zinc-100 bg-zinc-100 text-zinc-900"
                      : "border-line bg-panel text-zinc-300 hover:text-zinc-100"
                  }`}
                >
                  {item.label}
                </button>
              ))}
            </div>
            <div className="mt-2 rounded-xl border border-line bg-appbg/50 px-3 py-2 text-xs text-zinc-400">
              {capabilitySummary}
            </div>
            <button
              type="button"
              onClick={onToggleAutoClaim}
              disabled={!workerName.trim()}
              className={`mt-3 inline-flex h-10 items-center rounded-full border px-4 text-sm transition ${
                autoClaimEnabled
                  ? "border-emerald-400/40 bg-emerald-400/10 text-emerald-200"
                  : "border-line bg-panel text-zinc-300 hover:text-zinc-100"
              } disabled:cursor-not-allowed disabled:opacity-40`}
            >
              {autoClaimEnabled ? t("auto_claim_on") : t("auto_claim_off")}
            </button>
          </div>
        </div>

        <div className="mt-4 rounded-2xl border border-line bg-appbg/50 px-4 py-3 text-sm text-zinc-300">
          {statusSummary}
        </div>
        <div className="mt-3 grid gap-3 text-sm sm:grid-cols-3">
          <div className="rounded-2xl border border-line bg-appbg/50 p-3">
            <div className="text-xs text-zinc-500">{t("success_rate")}</div>
            <div className="mt-1 text-xl text-zinc-100">
              {`${Math.round(Number(orderStats.successRate || 0) * 100)}%`}
            </div>
          </div>
          <div className="rounded-2xl border border-line bg-appbg/50 p-3">
            <div className="text-xs text-zinc-500">{t("avg_first_token")}</div>
            <div className="mt-1 text-xl text-zinc-100">
              {orderStats.avgFirstTokenMs > 0 ? `${Number(orderStats.avgFirstTokenMs).toFixed(0)} ms` : "--"}
            </div>
          </div>
          <div className="rounded-2xl border border-line bg-appbg/50 p-3">
            <div className="text-xs text-zinc-500">{t("fail_top")}</div>
            <div className="mt-1 text-sm text-zinc-100 truncate" title={orderStats.failureTop?.[0] || ""}>
              {orderStats.failureTop
                ? `${orderStats.failureTop[0]} (${orderStats.failureTop[1]})`
                : "--"}
            </div>
          </div>
        </div>
        <div className="mt-4 grid gap-3 text-sm sm:grid-cols-4">
          <div className="rounded-2xl border border-line bg-appbg/50 p-3">
            <div className="text-xs text-zinc-500">{t("mine_total")}</div>
            <div className="mt-1 text-xl text-zinc-100">{orderStats.totalMine}</div>
          </div>
          <div className="rounded-2xl border border-line bg-appbg/50 p-3">
            <div className="text-xs text-zinc-500">{t("mine_active")}</div>
            <div className="mt-1 text-xl text-zinc-100">{orderStats.activeMine}</div>
          </div>
          <div className="rounded-2xl border border-line bg-appbg/50 p-3">
            <div className="text-xs text-zinc-500">{t("mine_done_failed")}</div>
            <div className="mt-1 text-xl text-zinc-100">{orderStats.completedMine} / {orderStats.failedMine}</div>
          </div>
          <div className="rounded-2xl border border-line bg-appbg/50 p-3">
            <div className="text-xs text-zinc-500">{t("local_models_ready")}</div>
            <div className="mt-1 text-xl text-zinc-100">{orderStats.installedReady}</div>
          </div>
        </div>
      </section>

      <section className="rounded-3xl border border-line bg-panel/40 p-5">
        <div>
          <div className="mb-4 flex items-center justify-between gap-3">
            <div>
              <h3 className="text-lg font-medium text-zinc-100">{visibleTitle}</h3>
              <p className="mt-1 text-sm text-zinc-500">{visibleDescription}</p>
            </div>
            <div className="text-sm text-zinc-400">
              {orderView === "mine"
                ? `${orderStats.totalMine} ${t("orders_word")}`
                : `${eligibleOrderCount} / ${availableOrders.length} ${t("eligible_suffix")}`}
            </div>
          </div>
          <div className="mb-4 flex flex-wrap gap-2">
            {ORDER_VIEW_OPTIONS.map((item) => (
              <button
                key={item.key}
                type="button"
                onClick={() => setOrderView(item.key)}
                className={`rounded-full border px-3 py-2 text-xs transition ${
                  orderView === item.key
                    ? "border-zinc-100 bg-zinc-100 text-zinc-900"
                    : "border-line bg-panel text-zinc-300 hover:text-zinc-100"
                }`}
              >
                {t(`view_${item.key}`)}
              </button>
            ))}
          </div>
          <div className="space-y-3">
            {visibleOrdersLimited.length === 0 ? (
              <div className="rounded-2xl border border-line bg-appbg/40 px-4 py-6 text-sm text-zinc-500">
                {orderView === "mine" ? t("empty_mine") : t("empty_orders")}
              </div>
            ) : (
              visibleOrdersLimited.map((item) => {
                const eligible = isOrderEligible(item);
                return (
                  <OrderRow
                    key={item.id}
                    item={item}
                    muted={!eligible}
                    action={
                      <button
                        type="button"
                        onClick={() => onClaim(item.id)}
                        disabled={
                          orderView === "mine" ||
                          !workerName.trim() ||
                          claimLoadingId === item.id ||
                          !eligible
                        }
                        className="rounded-xl bg-zinc-100 px-4 py-2 text-sm font-medium text-zinc-900 transition hover:bg-zinc-200 disabled:cursor-not-allowed disabled:opacity-40"
                      >
                        {orderView === "mine"
                          ? t("claim_done")
                          : claimLoadingId === item.id
                            ? t("claim_ing")
                            : eligible
                              ? t("claim_action")
                              : installedModelSet.has(item.model)
                                ? t("over_capability")
                                : t("model_missing")}
                      </button>
                    }
                  />
                );
              })
            )}
          </div>
        </div>
      </section>
    </div>
  );
}

function Composer({
  value,
  onChange,
  onSubmit,
  onStop,
  disabled,
  streaming,
  deepThink,
  onToggleDeepThink,
  imageName,
  imagePreview,
  uploadError,
  onPickImage,
  onClearImage,
  t,
}) {
  const areaRef = useRef(null);
  const fileInputRef = useRef(null);

  useEffect(() => {
    if (!areaRef.current) return;
    areaRef.current.style.height = "0px";
    areaRef.current.style.height = `${Math.min(areaRef.current.scrollHeight, 180)}px`;
  }, [value]);

  return (
    <footer className="border-t border-line p-4 sm:p-6">
      <div className="mx-auto flex w-full max-w-3xl flex-col gap-2 rounded-2xl border border-line bg-panel p-3">
        {imageName && (
          <div className="flex items-center gap-3 text-xs text-zinc-300">
            {imagePreview && (
              <img
                src={imagePreview}
                alt={imageName}
                className="h-12 w-12 rounded-xl border border-line object-cover"
              />
            )}
            <span className="rounded-full border border-line bg-appbg px-3 py-1">{imageName}</span>
            <button
              type="button"
              onClick={onClearImage}
              className="rounded-full border border-line px-2 py-1 text-[11px] text-zinc-400 transition hover:text-zinc-100"
            >
              {t("remove")}
            </button>
          </div>
        )}
        {uploadError && <div className="text-xs text-red-400">{uploadError}</div>}
        <div className="flex items-end gap-3">
          <input
            ref={fileInputRef}
            type="file"
            accept="image/*"
            className="hidden"
            onChange={(event) => {
              const file = event.target.files?.[0];
              if (file) onPickImage(file);
              event.target.value = "";
            }}
          />
          <button
            type="button"
            onClick={() => fileInputRef.current?.click()}
            className="h-11 rounded-xl border border-line px-4 text-sm text-zinc-300 transition hover:text-zinc-100"
          >
            +
          </button>
          <button
            type="button"
            onClick={onToggleDeepThink}
            className={`h-11 rounded-xl border px-4 text-sm transition ${
              deepThink
                ? "border-zinc-200 bg-zinc-100 text-zinc-900"
                : "border-line text-zinc-300 hover:text-zinc-100"
            }`}
          >
            {t("deep_think")} {deepThink ? t("on") : t("off")}
          </button>
        <textarea
          ref={areaRef}
          value={value}
          onChange={(event) => onChange(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter" && !event.shiftKey) {
              event.preventDefault();
              onSubmit();
            }
          }}
          rows={1}
          placeholder={t("composer_placeholder")}
          className="max-h-[180px] min-h-[44px] flex-1 resize-none bg-transparent px-1 py-2 text-sm text-zinc-100 outline-none placeholder:text-zinc-500"
        />
        <button
          type="button"
          disabled={streaming ? false : disabled || (!value.trim() && !imageName)}
          onClick={streaming ? onStop : onSubmit}
          className={`flex h-11 w-11 items-center justify-center rounded-full transition ${
            streaming
              ? "border border-red-300/40 bg-red-100 text-red-700 hover:bg-red-200"
              : "bg-zinc-100 text-zinc-900 hover:bg-zinc-200 disabled:cursor-not-allowed disabled:opacity-40"
          }`}
          title={streaming ? t("stop_generating") : t("send")}
        >
          {streaming ? <span className="h-3.5 w-3.5 rounded-[4px] bg-current" /> : "→"}
        </button>
        </div>
      </div>
    </footer>
  );
}

function LoginGate({
  t,
  loading,
  error,
  onGoogle,
  onGithub,
  onSendEmailOtp,
  onSendPhoneOtp,
  onVerifyEmailOtp,
  onVerifyPhoneOtp,
  email,
  setEmail,
  phone,
  setPhone,
  otp,
  setOtp,
  configMissing,
}) {
  return (
    <div className="mx-auto flex h-screen w-full max-w-6xl items-center justify-center p-4">
      <div className="w-full max-w-xl rounded-2xl border border-line bg-panel p-6">
        <h1 className="mb-2 text-xl font-semibold">{t("login_title")}</h1>
        <p className="mb-5 text-sm text-zinc-400">{configMissing ? t("login_missing_cfg") : t("login_subtitle")}</p>
        {error ? <div className="mb-4 rounded-lg border border-red-400/30 bg-red-500/10 px-3 py-2 text-sm text-red-300">{t("login_error_prefix", { error })}</div> : null}
        <div className="grid gap-3">
          <button type="button" disabled={loading || configMissing} onClick={onGoogle} className="h-11 rounded-xl border border-line text-sm hover:bg-zinc-900/60 disabled:opacity-40">{t("login_google")}</button>
          <button type="button" disabled={loading || configMissing} onClick={onGithub} className="h-11 rounded-xl border border-line text-sm hover:bg-zinc-900/60 disabled:opacity-40">{t("login_github")}</button>
        </div>
        <div className="my-5 h-px bg-line" />
        <div className="grid gap-3">
          <input value={email} onChange={(e) => setEmail(e.target.value)} placeholder={t("login_email_label")} className="h-11 rounded-xl border border-line bg-transparent px-3 text-sm outline-none" />
          <div className="grid grid-cols-2 gap-2">
            <button type="button" disabled={loading || configMissing || !email.trim()} onClick={onSendEmailOtp} className="h-10 rounded-lg border border-line text-sm hover:bg-zinc-900/60 disabled:opacity-40">{t("login_send_email")}</button>
            <button type="button" disabled={loading || configMissing || !email.trim() || !otp.trim()} onClick={onVerifyEmailOtp} className="h-10 rounded-lg border border-line text-sm hover:bg-zinc-900/60 disabled:opacity-40">{t("login_verify_email")}</button>
          </div>
          <input value={phone} onChange={(e) => setPhone(e.target.value)} placeholder={t("login_phone_label")} className="h-11 rounded-xl border border-line bg-transparent px-3 text-sm outline-none" />
          <input value={otp} onChange={(e) => setOtp(e.target.value)} placeholder={t("login_otp_label")} className="h-11 rounded-xl border border-line bg-transparent px-3 text-sm outline-none" />
          <div className="grid grid-cols-2 gap-2">
            <button type="button" disabled={loading || configMissing || !phone.trim()} onClick={onSendPhoneOtp} className="h-10 rounded-lg border border-line text-sm hover:bg-zinc-900/60 disabled:opacity-40">{t("login_send_phone")}</button>
            <button type="button" disabled={loading || configMissing || !phone.trim() || !otp.trim()} onClick={onVerifyPhoneOtp} className="h-10 rounded-lg border border-line text-sm hover:bg-zinc-900/60 disabled:opacity-40">{t("login_verify_phone")}</button>
          </div>
        </div>
      </div>
    </div>
  );
}

function App() {
  const chatRef = useRef(null);
  const bottomRef = useRef(null);
  const pollersRef = useRef(new Map());
  const dashboardTimerRef = useRef(null);
  const ordersTimerRef = useRef(null);
  const ordersEventSourceRef = useRef(null);
  const createTaskAbortRef = useRef(null);
  const authSyncPromiseRef = useRef(null);
  const authSyncTokenRef = useRef("");
  const bootstrapHashAuth = parseHashAuthParams(window.location.hash);
  const bootstrapAccessToken = String(bootstrapHashAuth.access_token || "").trim();
  const initialChatState = useMemo(() => getInitialChatState(), []);
  const [modelOptions, setModelOptions] = useState(() => {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (!raw) return DEFAULT_MODELS;
      const parsed = JSON.parse(raw);
      const saved = Array.isArray(parsed) ? parsed.filter(isAllowedModel) : [];
      return Array.from(new Set([...saved]));
    } catch {
      return DEFAULT_MODELS;
    }
  });
  const [model, setModel] = useState(() => {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (!raw) return "qwen3.5:9b"; // Default to 9b model
      const parsed = JSON.parse(raw);
      const saved = Array.isArray(parsed) ? parsed.filter(isAllowedModel) : [];
      return saved[0] || "qwen3.5:9b";
    } catch {
      return "qwen3.5:9b";
    }
  });
  const [mode, setMode] = useState("Auto");
  const [theme, setTheme] = useState(getInitialTheme);
  const [lang, setLang] = useState(getInitialLanguage);
  const [page, setPage] = useState(getInitialPage);
  const [deepThink, setDeepThink] = useState(false);
  const [credits, setCredits] = useState(0);
  const [supabaseClient] = useState(() => createSupabaseClient());
  const [loginError, setLoginError] = useState("");
  const [loginBusy, setLoginBusy] = useState(false);
  const [emailInput, setEmailInput] = useState("");
  const [phoneInput, setPhoneInput] = useState("");
  const [otpInput, setOtpInput] = useState("");
  const [isLoggedIn, setIsLoggedIn] = useState(false);
  const explicitSignOutRef = useRef(false);
  const [authToken, setAuthToken] = useState(() => {
    try {
      return (localStorage.getItem(AUTH_TOKEN_KEY) || "").trim();
    } catch {
      return "";
    }
  });
  const [authReady, setAuthReady] = useState(false);
  const [composer, setComposer] = useState("");
  const [imageAttachment, setImageAttachment] = useState(null);
  const [uploadError, setUploadError] = useState("");
  const [streaming, setStreaming] = useState(false);
  const [activeAssistantId, setActiveAssistantId] = useState(null);
  const [modelCommandBusy, setModelCommandBusy] = useState(false);
  const [modelCommandStatus, setModelCommandStatus] = useState("");
  const [installedModels, setInstalledModels] = useState([]);
  const [ollamaRuntime, setOllamaRuntime] = useState(null);
  const [ollamaInstallBusy, setOllamaInstallBusy] = useState(false);

  const [workerName, setWorkerName] = useState(getWorkerName);
  const [localMinerProfile, setLocalMinerProfile] = useState(null);
  const [effectiveMinerName, setEffectiveMinerName] = useState("");
  const [autoClaimEnabled, setAutoClaimEnabled] = useState(getAutoClaimEnabled);
  const [claimCapabilityPreference, setClaimCapabilityPreference] = useState(getClaimCapabilityPreference);
  const [minerProfile, setMinerProfile] = useState(null);
  const [availableOrders, setAvailableOrders] = useState([]);
  const [myOrders, setMyOrders] = useState([]);
  const [myOrdersSummary, setMyOrdersSummary] = useState(null);
  const [localMineOrders, setLocalMineOrders] = useState([]);
  const [ordersLoading, setOrdersLoading] = useState(false);
  const [ordersError, setOrdersError] = useState("");
  const [dashboardMetrics, setDashboardMetrics] = useState(null);
  const [claimLoadingId, setClaimLoadingId] = useState("");
  const [chatSessions, setChatSessions] = useState(initialChatState.sessions);
  const [currentChatId, setCurrentChatId] = useState(initialChatState.currentChatId);
  const [messages, setMessages] = useState(initialChatState.messages);
  const t = useMemo(() => (key, vars) => translateText(lang, key, vars), [lang]);
  const hashBootstrapToken = String(parseHashAuthParams(window.location.hash).access_token || "").trim();
  const hasHashAccessToken = /(?:^|[#&])access_token=/.test(String(window.location.hash || ""));

  useEffect(() => {
    const timer = setTimeout(() => {
      setAuthReady((prev) => (prev ? prev : true));
    }, 3000);
    return () => clearTimeout(timer);
  }, []);

  const appendAuthDebugLog = (event, detail = "") => {
    try {
      const key = "heph-auth-debug-log";
      const current = JSON.parse(localStorage.getItem(key) || "[]");
      const next = Array.isArray(current) ? current : [];
      next.push({
        ts: new Date().toISOString(),
        event: String(event || ""),
        detail: String(detail || ""),
      });
      localStorage.setItem(key, JSON.stringify(next.slice(-80)));
    } catch {}
  };

  useEffect(() => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(modelOptions));
  }, [modelOptions]);

  useEffect(() => {
    if (!model && modelOptions.length > 0) {
      setModel(modelOptions[0]);
    }
  }, [model, modelOptions]);

  useEffect(() => {
    try {
      localStorage.setItem(THEME_KEY, theme);
    } catch {}
    document.body.dataset.theme = theme;
  }, [theme]);

  useEffect(() => {
    try {
      localStorage.setItem(LANG_KEY, lang);
    } catch {}
    document.documentElement.lang =
      lang === "zh" ? "zh-CN" : lang === "ja" ? "ja" : lang === "fr" ? "fr" : "en";
  }, [lang]);

  useEffect(() => {
    const rawHash = String(window.location.hash || "");
    if (/(?:^|[#&])access_token=|(?:^|[#&])error=|(?:^|[#&])error_description=/i.test(rawHash)) {
      // Preserve OAuth callback hash payload until auth bootstrap consumes it.
      return;
    }
    const next = page === "orders" ? "#orders" : "#chat";
    if (window.location.hash !== next) {
      window.history.replaceState(null, "", next);
    }
  }, [page]);

  useEffect(() => {
    try {
      localStorage.setItem(WORKER_KEY, workerName);
    } catch {}
  }, [workerName]);

  useEffect(() => {
    try {
      localStorage.setItem(AUTO_CLAIM_KEY, autoClaimEnabled ? "1" : "0");
    } catch {}
  }, [autoClaimEnabled]);

  useEffect(() => {
    try {
      localStorage.setItem(CLAIM_CAPABILITY_KEY, claimCapabilityPreference);
    } catch {}
  }, [claimCapabilityPreference]);

  useEffect(() => {
    try {
      localStorage.setItem(CHAT_SESSIONS_KEY, JSON.stringify(chatSessions));
      localStorage.setItem(CURRENT_CHAT_KEY, currentChatId);
    } catch {}
  }, [chatSessions, currentChatId]);

  const authHeaders = useMemo(() => {
    if (!authToken) return {};
    return { Authorization: `Bearer ${authToken}` };
  }, [authToken]);

  const applyOrdersSnapshot = (snapshot) => {
    if (!snapshot || typeof snapshot !== "object") return;
    setAvailableOrders(Array.isArray(snapshot.available_orders) ? snapshot.available_orders : []);
    setMyOrders(Array.isArray(snapshot.my_orders) ? snapshot.my_orders : []);
    setMyOrdersSummary(snapshot.my_orders_summary || null);
    setLocalMineOrders(Array.isArray(snapshot.local_mine_orders) ? snapshot.local_mine_orders : []);
    setMinerProfile(snapshot.profile || null);
    setDashboardMetrics(snapshot.metrics || null);
    setOrdersError("");
    setOrdersLoading(false);
  };

  const refreshOllamaRuntime = async () => {
    if (!authToken) return;
    try {
      const response = await fetch(`${API_BASE}/ollama/runtime`, {
        headers: { ...authHeaders },
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || payload?.status !== "success") return;
      setOllamaRuntime(payload);
    } catch {}
  };

  const installOrRepairOllama = async () => {
    if (!authToken || ollamaInstallBusy) return;
    setOllamaInstallBusy(true);
    setModelCommandStatus("");
    try {
      const response = await fetch(`${API_BASE}/ollama/install`, {
        method: "POST",
        headers: { "Content-Type": "application/json", ...authHeaders },
        body: JSON.stringify({ force: true }),
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || payload?.status !== "success") {
        throw new Error(getApiErrorMessage(payload, t("ollama_install_failed"), lang));
      }
      setOllamaRuntime(payload);
      setModelCommandStatus(payload?.message || t("ollama_installer_opened"));
    } catch (error) {
      setModelCommandStatus(error.message || t("ollama_install_failed"));
    } finally {
      setOllamaInstallBusy(false);
    }
  };

  const jumpToChatAfterLogin = () => {
    setPage("chat");
    if (window.location.hash !== "#chat") {
      window.history.replaceState(null, "", "#chat");
    }
  };

  const syncGatewaySession = async (accessToken, refreshToken = "") => {
    const token = String(accessToken || "").trim();
    const refresh = String(refreshToken || "").trim();
    if (!token) throw new Error("missing access token");
    if (authSyncPromiseRef.current && authSyncTokenRef.current === token) {
      return await authSyncPromiseRef.current;
    }
    authSyncTokenRef.current = token;
    authSyncPromiseRef.current = (async () => {
      return await exchangeSupabaseSession(token);
    })();
    try {
      return await authSyncPromiseRef.current;
    } finally {
      authSyncPromiseRef.current = null;
      authSyncTokenRef.current = "";
    }
  };

  const acceptSupabaseTokenLogin = async (accessToken, hintedUserId = "", refreshToken = "") => {
    const token = String(accessToken || "").trim();
    const refresh = String(refreshToken || "").trim();
    if (!token) throw new Error("missing access token");
    let nextUserId = String(hintedUserId || "").trim();
    if (!nextUserId && supabaseClient?.auth?.getUser) {
      try {
        const { data } = await supabaseClient.auth.getUser(token);
        nextUserId = String(data?.user?.id || "").trim();
      } catch {}
    }
    try {
      if (nextUserId) localStorage.setItem(USER_KEY, nextUserId);
      localStorage.setItem(AUTH_TOKEN_KEY, token);
    } catch {}
    storeSupabaseSessionTokens(token, refresh);
    clearSupabaseStoredSessionTokens();
    setAuthToken(token);
    setIsLoggedIn(true);
    appendAuthDebugLog("acceptSupabaseTokenLogin.fallback", nextUserId || "no-user-id");
    jumpToChatAfterLogin();
    return { status: "success", session: { token, user_id: nextUserId } };
  };

  const exchangeSupabaseSession = async (accessToken) => {
    if (!accessToken) throw new Error("missing access token");
    const response = await fetch(`${API_BASE}/auth/supabase/session`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${accessToken}`,
      },
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || payload?.status !== "success" || !payload?.session?.token) {
      throw new Error(payload?.message || `session exchange failed: ${response.status}`);
    }
    const nextUserId = payload.session.user_id;
    const nextToken = payload.session.token;
    try {
      localStorage.setItem(USER_KEY, nextUserId);
      localStorage.setItem(AUTH_TOKEN_KEY, nextToken);
    } catch {}
    clearSupabaseStoredSessionTokens();
    setAuthToken(nextToken);
    setCredits(Number(payload?.credits?.available || 0));
    setIsLoggedIn(true);
    jumpToChatAfterLogin();
    return payload;
  };

  const validateGatewaySessionToken = async (tokenCandidate) => {
    const token = String(tokenCandidate || "").trim();
    if (!token) return { ok: false, reason: "missing" };
    try {
      const response = await fetch(`${API_BASE}/credits/me`, {
        headers: {
          Authorization: `Bearer ${token}`,
        },
      });
      const payload = await response.json().catch(() => ({}));
      if (response.ok && payload?.status === "success" && payload?.credits) {
        setCredits(Number(payload.credits.available || 0));
        appendAuthDebugLog("gateway.validate.ok", token.slice(0, 12));
        return { ok: true, token };
      }
      if (response.status === 401 || response.status === 403 || payload?.code === "unauthorized") {
        appendAuthDebugLog("gateway.validate.unauthorized", `${response.status}`);
        return { ok: false, reason: "unauthorized" };
      }
      appendAuthDebugLog("gateway.validate.transient", `${response.status}`);
      return { ok: false, reason: "transient", payload, status: response.status };
    } catch (error) {
      appendAuthDebugLog("gateway.validate.network", String(error?.message || error));
      return { ok: false, reason: "network", error };
    }
  };

  const getStoredGatewayTokenCandidate = () => {
    const candidates = [];
    const current = String(authToken || "").trim();
    if (current) candidates.push(current);
    try {
      const stored = String(localStorage.getItem(AUTH_TOKEN_KEY) || "").trim();
      if (stored && !candidates.includes(stored)) {
        candidates.push(stored);
      }
    } catch {}
    return candidates;
  };

  const recoverExistingGatewaySession = async () => {
    const candidates = getStoredGatewayTokenCandidate();
    let sawIndeterminate = false;

    for (const candidate of candidates) {
      const validated = await validateGatewaySessionToken(candidate);
      if (validated.ok) {
        setAuthToken(candidate);
        setIsLoggedIn(true);
        jumpToChatAfterLogin();
        return { status: "valid", token: candidate };
      }
      if (validated.reason === "transient" || validated.reason === "network") {
        sawIndeterminate = true;
      }
    }
    if (sawIndeterminate && candidates.length > 0) {
      return { status: "indeterminate", token: candidates[0] };
    }
    return { status: "invalid", token: "" };
  };

  const keepGatewaySessionIfPossible = async () => {
    const recovered = await recoverExistingGatewaySession();
    if (recovered.status === "valid") {
      return true;
    }
    if (recovered.status === "indeterminate" && recovered.token) {
      setAuthToken(recovered.token);
      setIsLoggedIn(true);
      jumpToChatAfterLogin();
      return true;
    }
    return false;
  };

  const preserveStoredGatewaySession = () => {
    const candidates = getStoredGatewayTokenCandidate();
    const token = String(candidates[0] || "").trim();
    if (!token) return false;
    setAuthToken(token);
    setIsLoggedIn(true);
    appendAuthDebugLog("gateway.preserve_session", token.slice(0, 12));
    jumpToChatAfterLogin();
    return true;
  };

  const hasPersistentGatewaySession = () => {
    try {
      if (String(authToken || "").trim()) return true;
      if (String(localStorage.getItem(AUTH_TOKEN_KEY) || "").trim()) return true;
    } catch {}
    return false;
  };

  useEffect(() => {
    if (isLoggedIn) return;
    if (!hasPersistentGatewaySession()) return;
    appendAuthDebugLog("auth.restore_from_persistent_token");
    preserveStoredGatewaySession();
  }, [isLoggedIn, authToken]);

  const refreshCredits = async (token = authToken) => {
    const validated = await validateGatewaySessionToken(token);
    if (validated.ok) return true;
    if (validated.reason === "transient" || validated.reason === "network") {
      return Boolean(String(token || "").trim());
    }
    return false;
  };

  useEffect(() => {
    let cancelled = false;
    const bootstrapAuth = async () => {
      try {
        const hashAuth = parseHashAuthParams(window.location.hash);
        if (hashAuth.error || hashAuth.error_description) {
          setLoginError(hashAuth.error_description || hashAuth.error || "oauth error");
        }
        if (hashAuth.access_token) {
          try {
            await syncGatewaySession(hashAuth.access_token, hashAuth.refresh_token);
            window.history.replaceState(null, "", window.location.pathname + "#chat");
            if (!cancelled) setAuthReady(true);
            return;
          } catch (hashTokenError) {
            appendAuthDebugLog("bootstrap.hash_exchange_failed", String(hashTokenError?.message || hashTokenError));
            await acceptSupabaseTokenLogin(hashAuth.access_token, "", hashAuth.refresh_token);
            window.history.replaceState(null, "", window.location.pathname + "#chat");
            if (!cancelled) setAuthReady(true);
            return;
          }
        }

        const bootstrapToken = String(authToken || bootstrapAccessToken || "").trim();
        if (bootstrapToken) {
          const ok = await refreshCredits(bootstrapToken);
          if (ok) {
            setAuthToken(bootstrapToken);
            setIsLoggedIn(true);
            jumpToChatAfterLogin();
            if (!cancelled) setAuthReady(true);
            return;
          }
          if (preserveStoredGatewaySession()) {
            if (!cancelled) setAuthReady(true);
            return;
          }
        }

        const savedToken = (localStorage.getItem(AUTH_TOKEN_KEY) || "").trim();
        if (savedToken) {
          setAuthToken(savedToken);
          const ok = await refreshCredits(savedToken);
          if (ok) {
            setIsLoggedIn(true);
            if (!cancelled) setAuthReady(true);
            return;
          }
          if (preserveStoredGatewaySession()) {
            if (!cancelled) setAuthReady(true);
            return;
          }
          appendAuthDebugLog("bootstrap.saved_token_stale_preserved");
          setAuthToken(savedToken);
          setIsLoggedIn(true);
          jumpToChatAfterLogin();
          if (!cancelled) setAuthReady(true);
          return;
        }
        if (!supabaseClient) {
          setLoginError(t("login_missing_cfg"));
          preserveStoredGatewaySession();
          if (!cancelled) setAuthReady(true);
          return;
        }
        if (window.location.search && /(?:^|[?&])code=/.test(window.location.search)) {
          try {
            const { error: exchangeErr } = await supabaseClient.auth.exchangeCodeForSession(window.location.href);
            if (exchangeErr) throw exchangeErr;
            const { data } = await supabaseClient.auth.getSession();
            const accessToken = data?.session?.access_token || "";
            if (accessToken) {
              try {
                await syncGatewaySession(accessToken, data?.session?.refresh_token || "");
              } catch {
                appendAuthDebugLog("bootstrap.code_exchange_failed");
                await acceptSupabaseTokenLogin(
                  accessToken,
                  data?.session?.user?.id || "",
                  data?.session?.refresh_token || ""
                );
              }
            }
            clearSupabaseStoredSessionTokens();
            window.history.replaceState(null, "", window.location.pathname + "#chat");
            if (!cancelled) setAuthReady(true);
            return;
          } catch (exchangeError) {
            setLoginError(String(exchangeError?.message || exchangeError));
          }
        }
        const recovered = await keepGatewaySessionIfPossible();
        if (!recovered) {
          preserveStoredGatewaySession();
        }
      } catch (error) {
        if (!cancelled) {
          appendAuthDebugLog("bootstrap.error", String(error?.message || error));
          const recovered = await keepGatewaySessionIfPossible();
          if (!recovered && !preserveStoredGatewaySession()) {
            setLoginError(String(error?.message || error));
          }
        }
      } finally {
        if (!cancelled) setAuthReady(true);
      }
    };
    bootstrapAuth();
    return () => {
      cancelled = true;
    };
  }, [supabaseClient]);

  useEffect(() => {
    if (!authToken || !authReady || !isLoggedIn) return undefined;
    let stopped = false;
    const tick = async () => {
      if (stopped) return;
      await refreshCredits(authToken);
      if (stopped) return;
      setTimeout(tick, 10000);
    };
    tick();
    return () => {
      stopped = true;
    };
  }, [authReady, authToken]);

  useEffect(() => {
    setChatSessions((prev) =>
      prev.map((session) =>
        session.id === currentChatId
          ? {
              ...session,
              title: summarizeConversationTitle(messages),
              updatedAt: new Date().toISOString(),
              messages,
            }
          : session
      )
    );
  }, [messages, currentChatId]);

  useEffect(() => {
    if (page !== "orders") return undefined;

    let cancelled = false;
    const pollLocalMinerProfile = async () => {
      try {
        const response = await fetch(`${LOCAL_MINER_PROFILE_URL}?t=${Date.now()}`, {
          headers: { ...authHeaders },
        });
        if (!response.ok) throw new Error(`Local miner profile failed: ${response.status}`);
        const payload = await response.json();
        if (payload?.status !== "success") {
          throw new Error(payload?.message || "Local miner profile failed");
        }
        if (cancelled) return;
        const effectiveName = String(
          payload?.effective_miner_name ||
          payload?.profile?.effective_miner_name ||
          payload?.profile?.miner_name ||
          ""
        ).trim();
        setEffectiveMinerName(effectiveName);
        const profile = {
          ...(payload.profile || {}),
          found:
            typeof payload?.profile?.found === "boolean"
              ? payload.profile.found
              : true,
        };
        setLocalMinerProfile(profile);
        if (typeof payload?.profile?.auto_claim_enabled === "boolean") {
          setAutoClaimEnabled(payload.profile.auto_claim_enabled);
        }
        const boundName = String(profile.miner_name || effectiveName || "").trim();
        if (boundName) {
          setWorkerName((prev) => {
            const current = String(prev || "").trim();
            if (!current || /^user-[a-z0-9]{8}$/i.test(current)) {
              return boundName;
            }
            return prev;
          });
        }
      } catch (_error) {
        if (!cancelled) {
          setLocalMinerProfile(null);
          setEffectiveMinerName("");
        }
      } finally {
        if (!cancelled) {
          setTimeout(pollLocalMinerProfile, 5000);
        }
      }
    };

    pollLocalMinerProfile();
    return () => {
      cancelled = true;
    };
  }, [page, workerName, authHeaders]);

  useEffect(() => {
    if (!bottomRef.current || page !== "chat") return;
    bottomRef.current.scrollIntoView({ block: "end", behavior: "smooth" });
  }, [messages, page]);

  useEffect(() => {
    if (!authReady || !authToken) return;
    if (page !== "chat" && page !== "orders") return;
    runOllamaCommand("list");
  }, [authReady, authToken, page]);

  useEffect(() => {
    if (!authReady || !authToken) return;
    refreshOllamaRuntime();
  }, [authReady, authToken]);

  useEffect(() => () => {
    pollersRef.current.forEach((timer) => clearTimeout(timer));
    pollersRef.current.clear();

    if (ordersTimerRef.current) {
      clearTimeout(ordersTimerRef.current);
      ordersTimerRef.current = null;
    }
    if (ordersEventSourceRef.current) {
      ordersEventSourceRef.current.close();
      ordersEventSourceRef.current = null;
    }
  }, []);


  useEffect(() => {
    if (page !== "orders") {
      if (ordersTimerRef.current) {
        clearTimeout(ordersTimerRef.current);
        ordersTimerRef.current = null;
      }
      if (ordersEventSourceRef.current) {
        ordersEventSourceRef.current.close();
        ordersEventSourceRef.current = null;
      }
      return undefined;
    }
    if (!authReady || !authToken) {
      return undefined;
    }

    let cancelled = false;
    const resolveMinerNames = () => {
      const resolvedMinerName = String(
        effectiveMinerName ||
        localMinerProfile?.effective_miner_name ||
        localMinerProfile?.miner_name ||
        workerName
      ).trim();
      const localMinerName = String(localMinerProfile?.miner_name || "").trim();
      return { resolvedMinerName, localMinerName };
    };
    const loadOrdersSnapshot = async () => {
      setOrdersLoading(true);
      try {
        const { resolvedMinerName, localMinerName } = resolveMinerNames();
        const profileQuery = new URLSearchParams();
        if (resolvedMinerName) profileQuery.set("miner_name", resolvedMinerName);
        const securedGet = (url) => fetch(url, { headers: { ...authHeaders } });
        const localMinePromise = localMinerName
          ? securedGet(`${API_BASE}/orders/mine?miner_name=${encodeURIComponent(localMinerName)}&limit=200`)
          : Promise.resolve(null);
        const profilePromise = resolvedMinerName
          ? securedGet(`${API_BASE}/orders/profile?${profileQuery.toString()}`)
          : Promise.resolve(null);
        const [availableResponse, mineResponse, profileResponse, metricsResponse] = await Promise.all([
          securedGet(`${API_BASE}/orders?status=pending&source=frontend&limit=30`),
          securedGet(`${API_BASE}/orders/mine?miner_name=${encodeURIComponent(resolvedMinerName || workerName)}&limit=200`),
          profilePromise,
          securedGet(`${API_BASE}/dashboard/metrics?limit=500`),
        ]);
        const [availablePayload, minePayload, profilePayload, metricsPayload] = await Promise.all([
          availableResponse.json(),
          mineResponse.json(),
          profileResponse ? profileResponse.json() : Promise.resolve(null),
          metricsResponse.json(),
        ]);
        const localMineResponse = await localMinePromise;
        const localMinePayload = localMineResponse ? await localMineResponse.json() : null;
        if (!availableResponse.ok || availablePayload?.status !== "success") {
          throw new Error(getApiErrorMessage(availablePayload, "接单列表加载失败", lang));
        }
        const mineNotFound = isNotFoundApiPayload(minePayload);
        if ((!mineResponse.ok || minePayload?.status !== "success") && !mineNotFound) {
          throw new Error(getApiErrorMessage(minePayload, "我的接单列表加载失败", lang));
        }
        const softProfileError =
          profileResponse && !isNotFoundApiPayload(profilePayload) && (!profileResponse.ok || profilePayload?.status !== "success")
            ? getApiErrorMessage(profilePayload, "矿工信息加载失败", lang)
            : "";
        const softLocalMineError =
          localMineResponse && !isNotFoundApiPayload(localMinePayload) && (!localMineResponse.ok || localMinePayload?.status !== "success")
            ? getApiErrorMessage(localMinePayload, "本机接单列表加载失败", lang)
            : "";
        const softMetricsError =
          !isNotFoundApiPayload(metricsPayload) && (!metricsResponse.ok || metricsPayload?.status !== "success")
            ? getApiErrorMessage(metricsPayload, "统计数据加载失败", lang)
            : "";
        if (!cancelled) {
          applyOrdersSnapshot({
            available_orders: availablePayload.orders || [],
            my_orders: mineNotFound ? [] : (minePayload.orders || []),
            my_orders_summary: mineNotFound ? null : (minePayload.summary || null),
            local_mine_orders: softLocalMineError ? [] : (localMinePayload?.orders || []),
            profile: softProfileError ? null : (profilePayload?.profile || null),
            metrics: softMetricsError ? null : (metricsPayload?.metrics || null),
          });
          setOrdersError(softProfileError || softLocalMineError || softMetricsError || "");
        }
      } catch (error) {
        if (!cancelled) {
          setOrdersError(error?.message || "Orders load failed");
        }
      } finally {
        if (!cancelled) {
          setOrdersLoading(false);
        }
      }
    };

    const scheduleFallbackRefresh = () => {
      if (cancelled) return;
      if (ordersTimerRef.current) {
        clearTimeout(ordersTimerRef.current);
      }
      ordersTimerRef.current = setTimeout(async () => {
        await loadOrdersSnapshot();
        scheduleFallbackRefresh();
      }, 15000);
    };

    loadOrdersSnapshot();
    scheduleFallbackRefresh();

    const { resolvedMinerName, localMinerName } = resolveMinerNames();
    const streamQuery = new URLSearchParams({
      status: "pending",
      source: "frontend",
      limit: "30",
      metrics_limit: "500",
      auth_token: authToken,
    });
    if (resolvedMinerName) streamQuery.set("miner_name", resolvedMinerName);
    if (localMinerName) streamQuery.set("local_miner_name", localMinerName);
    const eventSource = new EventSource(`${API_BASE}/orders/stream?${streamQuery.toString()}`);
    ordersEventSourceRef.current = eventSource;

    eventSource.addEventListener("snapshot", (event) => {
      if (cancelled) return;
      try {
        const payload = JSON.parse(String(event.data || "{}"));
        if (payload?.status === "success" && payload?.snapshot) {
          applyOrdersSnapshot(payload.snapshot);
          setOrdersError("");
        }
      } catch (error) {
        console.error("orders snapshot parse failed", error);
      }
    });

    eventSource.addEventListener("error", () => {
      if (cancelled) return;
      loadOrdersSnapshot();
    });

    return () => {
      cancelled = true;
      if (ordersTimerRef.current) {
        clearTimeout(ordersTimerRef.current);
        ordersTimerRef.current = null;
      }
      if (ordersEventSourceRef.current) {
        ordersEventSourceRef.current.close();
        ordersEventSourceRef.current = null;
      }
    };
  }, [page, workerName, localMinerProfile, effectiveMinerName, authReady, authToken, authHeaders]);

  const effectiveClaimCapabilityScore = getCapabilityScoreFromPreference(claimCapabilityPreference, minerProfile);
  const installedModelSetForAutoClaim = useMemo(
    () => new Set((installedModels || []).map((item) => String(item || "").trim()).filter(Boolean)),
    [installedModels]
  );
  const claimWorkerName = useMemo(
    () =>
      String(
        effectiveMinerName ||
        localMinerProfile?.effective_miner_name ||
        localMinerProfile?.miner_name ||
        workerName
      ).trim(),
    [effectiveMinerName, localMinerProfile, workerName]
  );
  const filteredAvailableOrders = availableOrders.filter((item) => {
    if (!installedModelSetForAutoClaim.has(item.model)) return false;
    if (!effectiveClaimCapabilityScore) return true;
    return getModelCapabilityScore(item.model) <= effectiveClaimCapabilityScore;
  });

  useEffect(() => {
    if (page !== "orders") return;
    if (!autoClaimEnabled) return;
    if (!claimWorkerName) return;
    if (claimLoadingId) return;

    const hasActiveMine = myOrders.some((item) => ["claimed", "processing"].includes(String(item?.status || "").toLowerCase()));
    if (hasActiveMine) return;

    const nextPending = filteredAvailableOrders.find((item) => String(item?.status || "").toLowerCase() === "pending");
    if (!nextPending?.id) return;

    handleClaim(nextPending.id);
  }, [page, autoClaimEnabled, claimWorkerName, filteredAvailableOrders, myOrders, claimLoadingId]);

  const addModel = (name) => {
    const next = name.trim();
    if (!isAllowedModel(next)) return;
    setModelOptions((prev) => (prev.includes(next) ? prev : [...prev, next]));
    setModel(next);
  };

  const runOllamaCommand = async (action, modelName = "", command = "") => {
    const safeAction = String(action || "").trim().toLowerCase();
    const safeModel = String(modelName || "").trim();
    if (!["list", "pull", "rm"].includes(safeAction)) return;
    if ((safeAction === "pull" || safeAction === "rm") && !safeModel) return;

    setModelCommandBusy(true);
    setModelCommandStatus(
      safeAction === "list"
        ? ""
        : `正在执行 ollama ${safeAction} ${safeModel}...`
    );

    try {
      const response = await fetch(`${API_BASE}/models/ollama`, {
        method: "POST",
        headers: { "Content-Type": "application/json", ...authHeaders },
        body: JSON.stringify(command ? { command } : { action: safeAction, model: safeModel }),
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || payload?.status !== "success") {
        throw new Error(getApiErrorMessage(payload, payload?.stderr || "Ollama 操作失败", lang));
      }

      if (safeAction === "list") {
        const listed = parseOllamaList(payload.stdout);
        setInstalledModels(listed);
        if (listed.length > 0) {
          setModelOptions((prev) => Array.from(new Set([...prev, ...listed])));
          setModel((prev) => (prev && listed.includes(prev) ? prev : listed[0]));
        }
        setModelCommandStatus("");
      } else if (safeAction === "pull") {
        addModel(safeModel);
        setInstalledModels((prev) => Array.from(new Set([...prev, safeModel])));
        setModelCommandStatus(`安装完成：${safeModel}`);
      } else if (safeAction === "rm") {
        setModelOptions((prev) => {
          const next = prev.filter((item) => item !== safeModel);
          if (model === safeModel) {
            setModel(next[0] || "");
          }
          return next;
        });
        setInstalledModels((prev) => prev.filter((item) => item !== safeModel));
        setModelCommandStatus(`删除完成：${safeModel}`);
      }
      refreshOllamaRuntime();
    } catch (error) {
      setModelCommandStatus(error.message || "Ollama 操作失败");
    } finally {
      setModelCommandBusy(false);
    }
  };

  const toggleExpand = (id) => {
    setMessages((prev) => prev.map((item) => (item.id === id ? { ...item, expanded: !item.expanded } : item)));
  };

  const toggleReasoning = (id) => {
    setMessages((prev) =>
      prev.map((item) => (item.id === id ? { ...item, reasoningExpanded: !item.reasoningExpanded } : item))
    );
  };

  const stopPolling = (assistantId) => {
    const poller = pollersRef.current.get(assistantId);
    if (poller) {
      if (poller.type === "sse" && poller.eventSource) {
        poller.eventSource.close();
      } else if (typeof poller === "number") {
        clearTimeout(poller);
      }
      pollersRef.current.delete(assistantId);
    }
  };

  const schedulePoll = (assistantId, taskId, delayMs = 600) => {
    const safeDelay = Math.max(120, Math.min(5000, Number(delayMs) || 600));
    const timer = setTimeout(() => pollTask(assistantId, taskId), safeDelay);
    pollersRef.current.set(assistantId, timer);
  };

  const connectSSE = (assistantId, taskId, options = {}) => {
    const isLocal = Boolean(options?.isLocal);

    // Only use SSE for local tasks
    if (!isLocal) {
      schedulePoll(assistantId, taskId, 500);
      return;
    }

    let eventSource = null;
    try {
      const streamQuery = new URLSearchParams();
      const authToken = String(localStorage.getItem(AUTH_TOKEN_KEY) || "").trim();
      if (authToken) streamQuery.set("auth_token", authToken);
      const url = `${API_BASE}/task/${taskId}/stream${streamQuery.toString() ? `?${streamQuery.toString()}` : ""}`;
      eventSource = new EventSource(url);
      
      eventSource.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          const type = data.type;
          
          if (type === "status") {
            // Status update
            if (data.status === "cancelled") {
              stopPolling(assistantId);
              if (eventSource) {
                eventSource.close();
                eventSource = null;
              }
              setMessages((prev) =>
                prev.map((item) =>
                  item.id === assistantId
                    ? { ...item, status: "cancelled", content: item.content || "已停止生成。" }
                    : item
                )
              );
              setStreaming(false);
              setActiveAssistantId((current) => (current === assistantId ? null : current));
              refreshCredits();
            } else if (data.status === "processing") {
              // Task is processing, continue streaming
            }
          } else if (type === "delta") {
            // Streaming content update
            const delta = data.delta || "";
            setMessages((prev) =>
              prev.map((item) => {
                if (item.id !== assistantId) return item;
                let firstTokenMs = item.meta.firstTokenMs || 0;
                if (!firstTokenMs) {
                  firstTokenMs = item.meta.createdAtMs
                    ? Math.max(1, Date.now() - item.meta.createdAtMs)
                    : 1;
                }
                const elapsedMs = item.meta.createdAtMs
                  ? Math.max(firstTokenMs, Date.now() - item.meta.createdAtMs)
                  : Math.max(item.meta.elapsedMs || 0, firstTokenMs);
                return {
                  ...item,
                  content: delta,
                    meta: {
                      ...item.meta,
                      lastDelta: delta,
                      elapsedMs,
                      firstTokenMs,
                      hadStreamDelta: true,
                      localSlowWarningShown: false,
                    },
                  };
              })
            );
          } else if (type === "complete") {
            // Task completed
            stopPolling(assistantId);
            if (eventSource) {
              eventSource.close();
              eventSource = null;
            }
            const result = data.result || "";
            const completedAtMs = Date.now();
            setMessages((prev) =>
              prev.map((item) =>
                item.id === assistantId
                  ? {
                      ...item,
                      status: "completed",
                      content: result,
                      meta: {
                        ...item.meta,
                        elapsedMs:
                          Number(data.elapsed_ms || 0) > 0
                            ? Number(data.elapsed_ms || 0)
                            : item.meta.createdAtMs
                              ? Math.max(1, completedAtMs - item.meta.createdAtMs)
                              : item.meta.elapsedMs,
                        firstTokenMs:
                          Number(data.first_token_ms || 0) > 0
                            ? Number(data.first_token_ms || 0)
                            : item.meta.firstTokenMs > 0
                              ? item.meta.firstTokenMs
                              : item.meta.createdAtMs
                                ? Math.max(1, completedAtMs - item.meta.createdAtMs)
                                : 1,
                        lastDelta: result,
                        hadStreamDelta: true,
                        localSlowWarningShown: false,
                      },
                    }
                  : item
              )
            );
            setStreaming(false);
            setActiveAssistantId((current) => (current === assistantId ? null : current));
            refreshCredits();
          } else if (type === "error") {
            // Task failed
            stopPolling(assistantId);
            if (eventSource) {
              eventSource.close();
              eventSource = null;
            }
            const errorMsg = data.error || "未知错误";
            setMessages((prev) =>
              prev.map((item) =>
                item.id === assistantId
                  ? { ...item, status: "failed", content: `请求失败：${errorMsg}` }
                  : item
              )
            );
            setStreaming(false);
            setActiveAssistantId((current) => (current === assistantId ? null : current));
          }
        } catch (e) {
          console.error("SSE parse error:", e);
        }
      };

      eventSource.onerror = (error) => {
        console.error("SSE connection error:", error);
        // Fallback to polling on SSE error
        if (eventSource) {
          eventSource.close();
          eventSource = null;
        }
        schedulePoll(assistantId, taskId, 500);
      };

      // Store eventSource reference for cleanup
      pollersRef.current.set(assistantId, { type: "sse", eventSource });
    } catch (error) {
      console.error("Failed to create SSE connection:", error);
      // Fallback to polling
      schedulePoll(assistantId, taskId, 500);
    }
  };

  const applyTaskSnapshot = (assistantId, task) => {
    const taskStatus = String(task?.status || "processing").toLowerCase();
    const result = String(task?.result || "");
    const delta = String(task?.result_delta || "");
    const minerName = task?.miner_name || "";
    const createdAt = task?.created_at || null;
    const claimedAt = task?.claimed_at || null;
    const completedAt = task?.completed_at || null;
    const taskContext = task?.context && typeof task.context === "object" ? task.context : {};

    setMessages((prev) =>
      prev.map((item) => {
        if (item.id !== assistantId) return item;

        const previousDelta = item.meta.lastDelta || "";
        let nextContent = item.content;
        let firstTokenMs = item.meta.firstTokenMs || 0;
        let hadStreamDelta = Boolean(item.meta.hadStreamDelta);
        const previousParsed = parseReasoningContent(item.content);

        if (taskStatus === "processing" && delta && delta !== previousDelta) {
          nextContent = delta;
          hadStreamDelta = true;
          if (!firstTokenMs) {
            firstTokenMs = createdAt ? Math.max(1, Date.now() - parseMs(createdAt)) : 1;
          }
        }

        if (taskStatus === "completed") {
          nextContent = result || nextContent;
          if (!firstTokenMs && nextContent) {
            firstTokenMs =
              item.meta.firstTokenMs ||
              diffMs(createdAt, completedAt || createdAt) ||
              (item.meta.createdAtMs ? Math.max(1, Date.now() - item.meta.createdAtMs) : 1);
          }
        }

        const completedElapsedMs =
          taskStatus === "completed"
            ? Math.max(
                diffMs(createdAt, completedAt) || 0,
                Number(item.meta.elapsedMs || 0),
                item.meta.createdAtMs && (nextContent || hadStreamDelta)
                  ? Math.max(1, Date.now() - item.meta.createdAtMs)
                  : 0
              )
            : 0;

        const parsed = parseReasoningContent(nextContent);
        const nextExec =
          taskContext.execution_mode === "local"
            ? "Local"
            : taskContext.execution_mode === "remote"
              ? "Remote"
              : resolveExec(item.meta.exec, minerName);
        const reasoningJustFinished =
          item.reasoningExpanded &&
          Boolean(parsed.reasoning) &&
          !Boolean(previousParsed.answer.trim()) &&
          Boolean(parsed.answer.trim());
        const autoCollapseReasoning =
          reasoningJustFinished ||
          (item.status !== "completed" && taskStatus === "completed" && Boolean(parsed.reasoning));

        return {
          ...item,
          content: nextContent,
          status: taskStatus,
          reasoningExpanded: autoCollapseReasoning ? false : item.reasoningExpanded,
          meta: {
            ...item.meta,
            exec: nextExec,
            elapsedMs:
              taskStatus === "completed"
                ? completedElapsedMs
                : Math.max(item.meta.elapsedMs, createdAt ? Date.now() - parseMs(createdAt) : 0),
            firstTokenMs,
            credits: (() => {
              const billing = taskContext?.billing && typeof taskContext.billing === "object" ? taskContext.billing : {};
              const reserved = Number(billing.reserved || 0);
              const charged = Number(billing.charged || 0);
              if (charged > 0 || reserved > 0) {
                if (taskStatus === "completed" || taskStatus === "failed" || taskStatus === "cancelled") {
                  return Math.max(0, charged);
                }
                return Math.max(0, reserved || charged);
              }
              return Number(item.meta.credits || 0);
            })(),
            taskId: task?.id || item.meta.taskId,
            node: minerName || item.meta.node,
            lastDelta: taskStatus === "processing" ? delta : "",
            hadStreamDelta,
          },
        };
      })
    );
  };

  const pollTask = async (assistantId, taskId) => {
    try {
      const response = await fetch(`${API_BASE}/task/${taskId}`, {
        headers: {
          ...authHeaders,
        },
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || payload?.status === "error") {
        throw new Error(getApiErrorMessage(payload, `任务轮询失败(${response.status})`, lang));
      }
      const task = payload.task;
      if (!task) throw new Error("Task payload missing");

      applyTaskSnapshot(assistantId, task);

      const taskStatus = String(task.status || "").toLowerCase();
      const taskContext = task?.context && typeof task.context === "object" ? task.context : {};
      const taskBilling = taskContext?.billing && typeof taskContext.billing === "object" ? taskContext.billing : {};
      const createdAtMs = parseMs(task?.created_at || null);
      const nowMs = Date.now();
      const elapsedMs = createdAtMs > 0 ? Math.max(0, nowMs - createdAtMs) : 0;
      const firstTokenMs = Number(taskContext?.metrics?.first_token_ms || 0);
      const isLocalExecution = String(taskContext.execution_mode || "").toLowerCase() === "local";
      const modelName = String(task?.model || taskContext?.model || "").trim();
      const localNoTokenTimeoutMs = getLocalNoTokenTimeoutMs(modelName);
      const localNoTokenTimeoutSeconds = Math.max(1, Math.round(localNoTokenTimeoutMs / 1000));
      if (
        isLocalExecution &&
        (taskStatus === "pending" || taskStatus === "claimed" || taskStatus === "processing") &&
        firstTokenMs <= 0 &&
        elapsedMs >= localNoTokenTimeoutMs
      ) {
        setMessages((prev) =>
          prev.map((item) =>
            item.id === assistantId
              ? {
                  ...item,
                  content:
                    item.content ||
                    `本地推理较慢：${localNoTokenTimeoutSeconds} 秒仍未收到首 token，继续等待本地模型返回。`,
                  meta: {
                    ...item.meta,
                    credits: 0,
                    billingState: String(taskBilling.state || ""),
                    localSlowWarningShown: true,
                  },
                }
              : item
          )
        );
        schedulePoll(assistantId, taskId, 1200);
        return;
      }
      if (taskStatus === "completed") {
        stopPolling(assistantId);
        refreshCredits();
        setStreaming(false);
        setActiveAssistantId((current) => (current === assistantId ? null : current));
        return;
      }
      if (taskStatus === "cancelled") {
        stopPolling(assistantId);
        refreshCredits();
        setMessages((prev) =>
          prev.map((item) =>
            item.id === assistantId
              ? {
                  ...item,
                  status: "cancelled",
                  content: item.content || "已停止生成。",
                }
              : item
          )
        );
        setStreaming(false);
        setActiveAssistantId((current) => (current === assistantId ? null : current));
        return;
      }
      if (taskStatus === "failed") {
        stopPolling(assistantId);
        refreshCredits();
        setMessages((prev) =>
          prev.map((item) =>
            item.id === assistantId
              ? {
                  ...item,
                  status: "failed",
                  content: item.content || `请求失败：${String(task?.failure_reason || "unknown_failure")}`,
                }
              : item
          )
        );
        setStreaming(false);
        setActiveAssistantId((current) => (current === assistantId ? null : current));
        return;
      }
      const nextPollMs =
        taskStatus === "pending"
          ? 1200
          : taskStatus === "claimed"
            ? 700
            : taskStatus === "processing"
              ? 350
              : 800;
      schedulePoll(assistantId, taskId, nextPollMs);
    } catch (error) {
      stopPolling(assistantId);
      setMessages((prev) =>
        prev.map((item) =>
          item.id === assistantId
            ? {
                ...item,
                status: "failed",
                content: item.content || `请求失败：${error?.message || "未知错误"}`,
              }
            : item
        )
      );
      setStreaming(false);
      setActiveAssistantId((current) => (current === assistantId ? null : current));
      console.error(error);
    }
  };

  const handlePickImage = async (file) => {
    try {
      if (!String(file?.type || "").startsWith("image/")) {
        setUploadError("只支持图片文件。");
        return;
      }
      if (file.size > MAX_IMAGE_BYTES) {
        setUploadError("图片不能超过 5MB。");
        return;
      }
      const dataUrl = await readFileAsDataUrl(file);
      setImageAttachment({
        name: file.name,
        dataUrl,
      });
      setUploadError("");
    } catch (error) {
      setUploadError("图片读取失败，请重试。");
      console.error(error);
    }
  };

  const submitAssistantRequest = async ({
    prompt,
    attachment = null,
    history,
    assistantId,
    localTaskId = crypto.randomUUID(),
  }) => {
    if (!authReady || !authToken) {
      throw new Error("认证会话未就绪，请稍后重试");
    }
    const controller = new AbortController();
    const requestId = crypto.randomUUID();
    createTaskAbortRef.current = controller;
    setActiveAssistantId(assistantId);
    const taskPayload = {
      prompt: prompt || "请分析这张图片",
      image_url: attachment?.dataUrl || null,
      model,
      mode: normalizeMode(mode),
      deep_think: deepThink,
      context: {
        history,
      },
    };
    console.info("[task/create] request", {
      requestId,
      model: taskPayload.model,
      mode: taskPayload.mode,
      deep_think: taskPayload.deep_think,
      prompt_len: String(taskPayload.prompt || "").length,
      has_image: Boolean(taskPayload.image_url),
    });

    try {
      const response = await fetch(`${API_BASE}/task`, {
        method: "POST",
        headers: { "Content-Type": "application/json", "x-request-id": requestId, ...authHeaders },
        signal: controller.signal,
        body: JSON.stringify(taskPayload),
      });
      const payload = await response.json().catch(() => ({}));
      const traceId = payload?.trace_id || response.headers.get("x-request-id") || requestId;
      console.info("[task/create] response", {
        requestId,
        traceId,
        http_status: response.status,
        payload_status: payload?.status || "unknown",
        code: payload?.code || "",
      });
      if (!response.ok || payload?.status === "error") {
        const errMsg = getApiErrorMessage(payload, `任务创建失败(${response.status})`, lang);
        throw new Error(`${errMsg}${traceId ? ` [trace:${traceId}]` : ""}`);
      }
      if (payload?.credits) {
        setCredits(Number(payload.credits.available || 0));
      }
      const taskId = payload?.task_id || payload?.task?.id;
      if (!taskId) throw new Error("Task id missing");
      const createdTask = payload?.task && typeof payload.task === "object" ? payload.task : {};
      console.info("[task/create] createdTask", createdTask);

      setMessages((prev) =>
        prev.map((item) =>
          item.id === assistantId
            ? {
                ...item,
                meta: {
                  ...item.meta,
                  taskId,
                  exec: createdTask.execution_mode === "local" ? "Local" : createdTask.execution_mode === "remote" ? "Remote" : item.meta.exec,
                  node: createdTask.execution_mode === "local" ? "local-ollama" : item.meta.node,
                  hadStreamDelta: false,
                },
              }
            : item
        )
      );

      connectSSE(assistantId, taskId, {
        isLocal: String(createdTask.execution_mode || "").toLowerCase() === "local",
      });
    } catch (error) {
      if (error?.name === "AbortError") {
        setMessages((prev) =>
          prev.map((item) =>
            item.id === assistantId
              ? {
                  ...item,
                  status: "cancelled",
                  content: item.content || "已停止生成。",
                }
              : item
          )
        );
        setStreaming(false);
        setActiveAssistantId((current) => (current === assistantId ? null : current));
        return;
      }
      stopPolling(assistantId);
      setMessages((prev) =>
        prev.map((item) =>
          item.id === assistantId
            ? {
                ...item,
                status: "failed",
                content: item.content || `请求失败：${error?.message || "未知错误"}`,
              }
            : item
        )
      );
      setStreaming(false);
      setActiveAssistantId((current) => (current === assistantId ? null : current));
      console.error(error);
    } finally {
      if (createTaskAbortRef.current === controller) {
        createTaskAbortRef.current = null;
      }
    }
  };

  const handleSend = async () => {
    const prompt = composer.trim();
    if ((!prompt && !imageAttachment) || streaming || !authReady) return;
    if (!String(model || "").trim()) {
      setModelCommandStatus(t("model_missing_notice"));
      return;
    }

    const history = buildConversationHistory(messages);
    setStreaming(true);
    setComposer("");
    const attachment = imageAttachment;
    setImageAttachment(null);
    setUploadError("");
    const assistantId = crypto.randomUUID();
    const localTaskId = crypto.randomUUID();

    setMessages((prev) => [
      ...prev,
      {
        id: crypto.randomUUID(),
        role: "user",
        content: attachment ? `${prompt || "请分析这张图片"}\n[图片: ${attachment.name}]` : prompt,
        imageDataUrl: attachment?.dataUrl || "",
        imageName: attachment?.name || "",
      },
      {
        id: assistantId,
        role: "assistant",
        content: "",
        status: "processing",
        expanded: false,
        reasoningExpanded: deepThink,
        meta: {
          model,
          exec: normalizeMode(mode) === "auto" ? "Routing" : mode,
          deepThink,
          elapsedMs: 0,
          firstTokenMs: 0,
          createdAtMs: Date.now(),
          credits: 0,
          taskId: localTaskId,
          node: normalizeMode(mode) === "local" ? "localhost" : "pending",
          lastDelta: "",
          hadStreamDelta: false,
        },
      },
    ]);

    await submitAssistantRequest({ prompt, attachment, history, assistantId, localTaskId });
  };

  const handleStop = async () => {
    const assistantId = activeAssistantId;
    if (!assistantId) return;

    const activeMessage = messages.find((item) => item.id === assistantId);
    const taskId = activeMessage?.meta?.taskId;

    if (createTaskAbortRef.current) {
      createTaskAbortRef.current.abort();
      createTaskAbortRef.current = null;
    }

    stopPolling(assistantId);
    setMessages((prev) =>
      prev.map((item) =>
        item.id === assistantId
          ? {
              ...item,
              status: "cancelled",
              content: item.content || "已停止生成。",
            }
          : item
      )
    );
    setStreaming(false);
    setActiveAssistantId(null);

    if (!taskId) return;

    try {
      await fetch(`${API_BASE}/cancel`, {
        method: "POST",
        headers: { "Content-Type": "application/json", ...authHeaders },
        body: JSON.stringify({
          id: taskId,
        }),
      });
      refreshCredits();
    } catch (error) {
      console.error(error);
    }
  };

  const handleClaim = async (taskId) => {
    const normalizedWorker = claimWorkerName;
    if (!taskId || !normalizedWorker) return;
    setClaimLoadingId(taskId);
    try {
      const response = await fetch(`${API_BASE}/orders/claim`, {
        method: "POST",
        headers: { "Content-Type": "application/json", ...authHeaders },
        body: JSON.stringify({
          id: taskId,
          miner_name: normalizedWorker,
          installed_models: installedModels,
        }),
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || payload?.status !== "success") {
        throw new Error(getApiErrorMessage(payload, "接单失败", lang));
      }
      setAvailableOrders((prev) => prev.filter((item) => item.id !== taskId));
      setMyOrders((prev) => [payload.order, ...prev.filter((item) => item.id !== taskId)].slice(0, 30));
      setMyOrdersSummary((prev) => {
        const current = prev || {};
        const total = Number(current.total_count || 0);
        const claimed = Number(current.claimed_count || 0);
        const processing = Number(current.processing_count || 0);
        return {
          ...current,
          total_count: total + 1,
          claimed_count: claimed + 1,
          active_count: claimed + processing + 1,
        };
      });
      setOrdersError("");
    } catch (error) {
      setOrdersError(error?.message || "Claim failed");
    } finally {
      setClaimLoadingId("");
    }
  };

  const handleToggleAutoClaim = async () => {
    if (!workerName.trim()) return;
    const nextEnabled = !autoClaimEnabled;
    setAutoClaimEnabled(nextEnabled);
    try {
      await fetch(LOCAL_MINER_CONTROL_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ auto_claim_enabled: nextEnabled }),
      });
    } catch (error) {
      setOrdersError(error?.message || "Auto-claim toggle sync failed");
    }
  };

  const handleSelectChat = (chatId) => {
    if (streaming || !chatId || chatId === currentChatId) return;
    const nextChat = chatSessions.find((item) => item.id === chatId);
    if (!nextChat) return;
    setCurrentChatId(chatId);
    setMessages(nextChat.messages || []);
    setComposer("");
    setImageAttachment(null);
    setUploadError("");
    setActiveAssistantId(null);
  };

  const handleNewChat = () => {
    if (streaming) return;
    const next = createChatSession();
    setChatSessions((prev) => [next, ...prev]);
    setCurrentChatId(next.id);
    setMessages(next.messages);
    setComposer("");
    setImageAttachment(null);
    setUploadError("");
    setActiveAssistantId(null);
  };

  const handleRenameChat = (chatId) => {
    if (streaming) return;
    const target = chatSessions.find((item) => item.id === chatId);
    if (!target) return;
    const nextTitle = window.prompt(t("rename_chat_prompt"), target.title || t("new_chat"));
    if (nextTitle == null) return;
    const normalized = nextTitle.trim() || t("new_chat");
    setChatSessions((prev) =>
      prev.map((item) =>
        item.id === chatId
          ? {
              ...item,
              title: normalized,
              updatedAt: new Date().toISOString(),
            }
          : item
      )
    );
  };

  const handlePinChat = (chatId) => {
    if (streaming) return;
    setChatSessions((prev) =>
      prev.map((item) =>
        item.id === chatId
          ? {
              ...item,
              pinned: !item.pinned,
              updatedAt: new Date().toISOString(),
            }
          : item
      )
    );
  };

  const handleDeleteChat = (chatId) => {
    if (streaming) return;
    const target = chatSessions.find((item) => item.id === chatId);
    if (!target) return;
    const ok = window.confirm(t("delete_chat_confirm", { title: target.title || t("new_chat") }));
    if (!ok) return;
    if (chatSessions.length <= 1) {
      const next = createChatSession();
      setChatSessions([next]);
      setCurrentChatId(next.id);
      setMessages(next.messages);
      setComposer("");
      setImageAttachment(null);
      setUploadError("");
      setActiveAssistantId(null);
      return;
    }
    const remaining = chatSessions.filter((item) => item.id !== chatId);
    setChatSessions(remaining);
    if (chatId === currentChatId) {
      const nextCurrent = sortChatSessions(remaining)[0];
      setCurrentChatId(nextCurrent.id);
      setMessages(nextCurrent.messages || []);
      setComposer("");
      setImageAttachment(null);
      setUploadError("");
      setActiveAssistantId(null);
    }
  };

  const handleClearCurrentChat = () => {
    if (streaming) return;
    const ok = window.confirm(t("clear_chat_confirm"));
    if (!ok) return;
    setMessages([]);
    setComposer("");
    setImageAttachment(null);
    setUploadError("");
    setActiveAssistantId(null);
  };

  const handleExportCurrentChat = () => {
    const session = chatSessions.find((item) => item.id === currentChatId);
    const title = (session?.title || "chat").replace(/[\\/:*?"<>|]/g, "_");
    const lines = (messages || []).map((item) => {
      const role = item.role === "user" ? "User" : "Assistant";
      const content = item.role === "assistant" ? getMessageAnswerText(item) : String(item.content || "");
      return `## ${role}\n\n${content.trim()}\n`;
    }).filter(Boolean);
    downloadTextFile(`${title || "chat"}.md`, lines.join("\n"));
  };

  const handleCopyMessage = async (assistantId) => {
    const target = messages.find((item) => item.id === assistantId);
    const text = getMessageAnswerText(target);
    if (!text) return;
    try {
      await navigator.clipboard.writeText(text);
    } catch {
      const area = document.createElement("textarea");
      area.value = text;
      document.body.appendChild(area);
      area.select();
      document.execCommand("copy");
      area.remove();
    }
  };

  const handleRetryMessage = async (assistantId) => {
    if (streaming) return;
    const assistantIndex = messages.findIndex((item) => item.id === assistantId);
    if (assistantIndex <= 0) return;
    const assistantMessage = messages[assistantIndex];
    if (!assistantMessage || assistantMessage.role !== "assistant") return;

    let userIndex = -1;
    for (let index = assistantIndex - 1; index >= 0; index -= 1) {
      if (messages[index]?.role === "user") {
        userIndex = index;
        break;
      }
    }
    if (userIndex === -1) return;

    const userMessage = messages[userIndex];
    const prompt = String(userMessage.content || "").replace(/\n?\[图片:[^\]]+\]\s*$/u, "").trim();
    const attachment = userMessage.imageDataUrl
      ? {
          name: userMessage.imageName || "uploaded-image",
          dataUrl: userMessage.imageDataUrl,
        }
      : null;
    const history = buildConversationHistory(messages.slice(0, userIndex));
    const localTaskId = crypto.randomUUID();

    setStreaming(true);
    setActiveAssistantId(assistantId);
    setMessages((prev) =>
      prev.map((item) =>
        item.id === assistantId
          ? {
              ...item,
              content: "",
              status: "processing",
              expanded: false,
              reasoningExpanded: Boolean(item.meta?.deepThink),
              meta: {
                ...item.meta,
                elapsedMs: 0,
                firstTokenMs: 0,
                credits: 0,
                taskId: localTaskId,
                node: normalizeMode(item.meta?.exec) === "local" ? "localhost" : "pending",
                lastDelta: "",
                hadStreamDelta: false,
              },
            }
          : item
      )
    );

    await submitAssistantRequest({ prompt, attachment, history, assistantId, localTaskId });
  };

  const handleOAuthLogin = async (provider) => {
    if (!supabaseClient) {
      setLoginError(t("login_missing_cfg"));
      return;
    }
    try {
      setLoginBusy(true);
      setLoginError("");
      const redirectTo = `${window.location.origin}${window.location.pathname}`;
      const { error } = await supabaseClient.auth.signInWithOAuth({
        provider,
        options: { redirectTo },
      });
      if (error) throw error;
    } catch (error) {
      setLoginError(String(error?.message || error));
    } finally {
      setLoginBusy(false);
    }
  };

  const handleSendEmailOtp = async () => {
    if (!supabaseClient || !emailInput.trim()) return;
    try {
      setLoginBusy(true);
      setLoginError("");
      const { error } = await supabaseClient.auth.signInWithOtp({
        email: emailInput.trim(),
      });
      if (error) throw error;
    } catch (error) {
      setLoginError(String(error?.message || error));
    } finally {
      setLoginBusy(false);
    }
  };

  const handleSendPhoneOtp = async () => {
    if (!supabaseClient || !phoneInput.trim()) return;
    try {
      setLoginBusy(true);
      setLoginError("");
      const { error } = await supabaseClient.auth.signInWithOtp({
        phone: phoneInput.trim(),
      });
      if (error) throw error;
    } catch (error) {
      setLoginError(String(error?.message || error));
    } finally {
      setLoginBusy(false);
    }
  };

  const handleVerifyOtp = async (type) => {
    if (!supabaseClient || !otpInput.trim()) return;
    try {
      setLoginBusy(true);
      setLoginError("");
      const payload =
        type === "email"
          ? { email: emailInput.trim(), token: otpInput.trim(), type: "email" }
          : { phone: phoneInput.trim(), token: otpInput.trim(), type: "sms" };
      const { data, error } = await supabaseClient.auth.verifyOtp(payload);
      if (error) throw error;
      const accessToken = data?.session?.access_token || "";
      if (!accessToken) throw new Error("No access token");
      try {
        await syncGatewaySession(accessToken, data?.session?.refresh_token || "");
      } catch {
        appendAuthDebugLog("otp.session_exchange_failed");
        await acceptSupabaseTokenLogin(
          accessToken,
          data?.session?.user?.id || "",
          data?.session?.refresh_token || ""
        );
      }
      setOtpInput("");
    } catch (error) {
      setLoginError(String(error?.message || error));
    } finally {
      setLoginBusy(false);
    }
  };

  const handleSignOut = async () => {
    explicitSignOutRef.current = true;
    appendAuthDebugLog("signout.explicit");
    setLoginError("");
    try {
      if (supabaseClient?.auth?.signOut) {
        await supabaseClient.auth.signOut();
      }
    } catch (error) {
      setLoginError(String(error?.message || error));
    } finally {
      setIsLoggedIn(false);
      setAuthToken("");
      try {
        localStorage.removeItem(AUTH_TOKEN_KEY);
      } catch {}
      storeSupabaseSessionTokens("", "");
      clearSupabaseStoredSessionTokens();
      explicitSignOutRef.current = false;
    }
  };

  if (!authReady && !hashBootstrapToken && !hasHashAccessToken) {
    return (
      <div className="mx-auto grid h-screen w-full max-w-6xl place-items-center p-4 text-sm text-zinc-400">
        {t("thinking")}
      </div>
    );
  }

  const shouldShowLoginGate = authReady && !isLoggedIn && !hashBootstrapToken && !hasPersistentGatewaySession();

  if (shouldShowLoginGate) {
    return (
      <LoginGate
        t={t}
        loading={loginBusy}
        error={loginError}
        onGoogle={() => handleOAuthLogin("google")}
        onGithub={() => handleOAuthLogin("github")}
        onSendEmailOtp={handleSendEmailOtp}
        onSendPhoneOtp={handleSendPhoneOtp}
        onVerifyEmailOtp={() => handleVerifyOtp("email")}
        onVerifyPhoneOtp={() => handleVerifyOtp("phone")}
        email={emailInput}
        setEmail={setEmailInput}
        phone={phoneInput}
        setPhone={setPhoneInput}
        otp={otpInput}
        setOtp={setOtpInput}
        configMissing={!supabaseClient}
      />
    );
  }

  return (
    <div className="mx-auto flex h-screen w-full max-w-6xl p-0 sm:p-5">
      <div className="grid h-screen w-full grid-rows-[auto_1fr_auto] overflow-hidden border border-line bg-appbg sm:h-[calc(100vh-40px)] sm:rounded-3xl">
        <TopBar
          model={model}
          setModel={setModel}
          models={modelOptions}
          mode={mode}
          setMode={setMode}
          theme={theme}
          setTheme={setTheme}
          lang={lang}
          setLang={setLang}
          t={t}
          page={page}
          setPage={setPage}
          credits={credits}
          addModel={addModel}
          runOllamaCommand={runOllamaCommand}
          modelCommandBusy={modelCommandBusy}
          modelCommandStatus={modelCommandStatus}
          installOrRepairOllama={installOrRepairOllama}
          ollamaInstallBusy={ollamaInstallBusy}
          ollamaRuntime={ollamaRuntime}
          onSignOut={handleSignOut}
        />

        <main
          ref={chatRef}
          className={page === "chat" ? "overflow-hidden" : "overflow-y-auto px-4 py-5 sm:px-6"}
        >
          {page === "orders" ? (
        <OrdersPage
          workerName={workerName}
          setWorkerName={setWorkerName}
          localMinerProfile={localMinerProfile}
              autoClaimEnabled={autoClaimEnabled}
              onToggleAutoClaim={handleToggleAutoClaim}
              claimCapabilityPreference={claimCapabilityPreference}
              setClaimCapabilityPreference={setClaimCapabilityPreference}
              minerProfile={minerProfile}
              effectiveClaimCapabilityScore={effectiveClaimCapabilityScore}
              eligibleOrderCount={filteredAvailableOrders.length}
              availableOrders={availableOrders}
              myOrders={myOrders}
              myOrdersSummary={myOrdersSummary}
              localMineOrders={localMineOrders}
              loading={ordersLoading}
              error={ordersError}
              dashboardMetrics={dashboardMetrics}
              claimLoadingId={claimLoadingId}
              onClaim={handleClaim}
              installedModels={installedModels}
              t={t}
            />
          ) : (
            <div className="flex h-full overflow-hidden">
              <ChatSidebar
                sessions={chatSessions}
                currentChatId={currentChatId}
                onSelectChat={handleSelectChat}
                onNewChat={handleNewChat}
                onRenameChat={handleRenameChat}
                onDeleteChat={handleDeleteChat}
                onPinChat={handlePinChat}
                onClearChat={handleClearCurrentChat}
                onExportChat={handleExportCurrentChat}
                disabled={streaming}
                t={t}
              />
              <div className="flex min-w-0 flex-1 flex-col overflow-hidden">
                <div className="flex-1 overflow-y-auto px-4 py-5 sm:px-6">
                  {messages.length === 0 ? (
                    <div className="grid h-full place-items-center" />
                  ) : (
                    <div className="mx-auto flex w-full max-w-3xl flex-col gap-5">
                      {messages.map((msg) => (
                        <ChatMessage
                          key={msg.id}
                          msg={msg}
                          onRetry={() => handleRetryMessage(msg.id)}
                          onRegenerate={() => handleRetryMessage(msg.id)}
                          onCopy={() => handleCopyMessage(msg.id)}
                          onToggle={(kind) => () => {
                            if (kind === "reasoning") {
                              toggleReasoning(msg.id);
                              return;
                            }
                            toggleExpand(msg.id);
                          }}
                          t={t}
                        />
                      ))}
                      <div ref={bottomRef} />
                    </div>
                  )}
                </div>
              </div>
            </div>
          )}
        </main>

        {page === "chat" ? (
          <Composer
            value={composer}
            onChange={setComposer}
            onSubmit={handleSend}
            onStop={handleStop}
            disabled={streaming}
            streaming={streaming}
            deepThink={deepThink}
            onToggleDeepThink={() => setDeepThink((value) => !value)}
            imageName={imageAttachment?.name || ""}
            imagePreview={imageAttachment?.dataUrl || ""}
            uploadError={uploadError}
            onPickImage={handlePickImage}
            onClearImage={() => {
              setImageAttachment(null);
              setUploadError("");
            }}
            t={t}
          />
        ) : (
          <footer className="border-t border-line px-4 py-4 text-sm text-zinc-500 sm:px-6">
            {t("footer_orders")}
          </footer>
        )}
      </div>
    </div>
  );
}

ReactDOM.createRoot(document.getElementById("app")).render(<App />);
