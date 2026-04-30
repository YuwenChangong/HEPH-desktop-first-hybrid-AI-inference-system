!include "MUI2.nsh"
!include "LogicLib.nsh"
!include "nsDialogs.nsh"

!define INSTALLER_LEGAL_DIR "${__FILEDIR__}\legal-installer"
!define LANG_ID_EN 1033
!define LANG_ID_ZH_CN 2052
!define LANG_ID_JA 1041
!define LANG_ID_FR 1036

Var OllamaMode
Var OllamaPage
Var OllamaOptionLight
Var OllamaOptionFull

LicenseLangString MUILicense ${LANG_ID_EN} "${INSTALLER_LEGAL_DIR}\installer-agreement.en.txt"
LicenseLangString MUILicense ${LANG_ID_ZH_CN} "${INSTALLER_LEGAL_DIR}\installer-agreement.zh-CN.txt"
LicenseLangString MUILicense ${LANG_ID_JA} "${INSTALLER_LEGAL_DIR}\installer-agreement.ja.txt"
LicenseLangString MUILicense ${LANG_ID_FR} "${INSTALLER_LEGAL_DIR}\installer-agreement.fr.txt"

LangString OllamaPageTitle ${LANG_ID_EN} "Choose Ollama Runtime"
LangString OllamaPageTitle ${LANG_ID_ZH_CN} "选择 Ollama 运行时"
LangString OllamaPageTitle ${LANG_ID_JA} "Ollama ランタイムを選択"
LangString OllamaPageTitle ${LANG_ID_FR} "Choisir le runtime Ollama"

LangString OllamaPageSubtitle ${LANG_ID_EN} "Select how the application should prepare the local model runtime."
LangString OllamaPageSubtitle ${LANG_ID_ZH_CN} "选择应用应如何准备本地模型运行时。"
LangString OllamaPageSubtitle ${LANG_ID_JA} "ローカルモデル実行環境の準備方法を選択してください。"
LangString OllamaPageSubtitle ${LANG_ID_FR} "Choisissez comment l'application doit préparer le runtime de modèles local."

LangString OllamaLightTitle ${LANG_ID_EN} "Lightweight Ollama (recommended)"
LangString OllamaLightTitle ${LANG_ID_ZH_CN} "轻量 Ollama（推荐）"
LangString OllamaLightTitle ${LANG_ID_JA} "軽量 Ollama（推奨）"
LangString OllamaLightTitle ${LANG_ID_FR} "Ollama léger (recommandé)"

LangString OllamaLightDesc ${LANG_ID_EN} "Use the bundled lightweight runtime first. Lower setup overhead, suitable for quick start."
LangString OllamaLightDesc ${LANG_ID_ZH_CN} "优先使用内置轻量运行时。安装负担更小，适合快速开始。"
LangString OllamaLightDesc ${LANG_ID_JA} "同梱の軽量ランタイムを優先利用します。初期負担が小さく、すぐに開始できます。"
LangString OllamaLightDesc ${LANG_ID_FR} "Utilise d'abord le runtime léger intégré. Mise en route plus simple et plus rapide."

LangString OllamaFullTitle ${LANG_ID_EN} "Full Ollama"
LangString OllamaFullTitle ${LANG_ID_ZH_CN} "完整 Ollama"
LangString OllamaFullTitle ${LANG_ID_JA} "完全版 Ollama"
LangString OllamaFullTitle ${LANG_ID_FR} "Ollama complet"

LangString OllamaFullDesc ${LANG_ID_EN} "Prefer a system Ollama installation when available. Better for users who want the full local runtime."
LangString OllamaFullDesc ${LANG_ID_ZH_CN} "如可用则优先使用系统安装的 Ollama。适合需要完整本地运行时的用户。"
LangString OllamaFullDesc ${LANG_ID_JA} "利用可能な場合はシステムに導入済みの Ollama を優先します。完全なローカルランタイムを使いたい方向けです。"
LangString OllamaFullDesc ${LANG_ID_FR} "Privilégie une installation système d'Ollama lorsqu'elle existe. Adapté aux utilisateurs voulant le runtime complet."

!macro customInit
  StrCpy $LANGUAGE ${LANG_ID_EN}
  StrCpy $OllamaMode "light"
  !insertmacro MUI_LANGDLL_DISPLAY
!macroend

!macro customInstallMode
  StrCpy $isForceCurrentInstall "1"
!macroend

!macro licensePage
  !insertmacro MUI_PAGE_LICENSE "$(MUILicense)"
!macroend

Function OllamaModePageCreate
  !pragma warning disable 6010
  !insertmacro MUI_HEADER_TEXT "$(OllamaPageTitle)" "$(OllamaPageSubtitle)"
  nsDialogs::Create 1018
  Pop $OllamaPage

  ${NSD_CreateRadioButton} 0u 12u 300u 12u "$(OllamaLightTitle)"
  Pop $OllamaOptionLight
  ${NSD_Check} $OllamaOptionLight

  ${NSD_CreateLabel} 16u 28u 300u 24u "$(OllamaLightDesc)"
  Pop $0

  ${NSD_CreateRadioButton} 0u 64u 300u 12u "$(OllamaFullTitle)"
  Pop $OllamaOptionFull

  ${NSD_CreateLabel} 16u 80u 300u 24u "$(OllamaFullDesc)"
  Pop $0

  nsDialogs::Show
FunctionEnd

Function OllamaModePageLeave
  ${NSD_GetState} $OllamaOptionFull $0
  ${If} $0 == ${BST_CHECKED}
    StrCpy $OllamaMode "full"
  ${Else}
    StrCpy $OllamaMode "light"
  ${EndIf}
FunctionEnd
!pragma warning default 6010

!macro customPageAfterChangeDir
  Page custom OllamaModePageCreate OllamaModePageLeave
!macroend

!macro customInstall
  StrCpy $0 "en"
  ${If} $LANGUAGE == ${LANG_ID_ZH_CN}
    StrCpy $0 "zh"
  ${ElseIf} $LANGUAGE == ${LANG_ID_JA}
    StrCpy $0 "ja"
  ${ElseIf} $LANGUAGE == ${LANG_ID_FR}
    StrCpy $0 "fr"
  ${EndIf}

  FileOpen $1 "$INSTDIR\installer-language.txt" w
  FileWrite $1 "$0"
  FileClose $1

  CreateDirectory "$INSTDIR\resources\runtime"
  FileOpen $1 "$INSTDIR\resources\runtime\ollama-install-mode.txt" w
  FileWrite $1 "$OllamaMode"
  FileClose $1
!macroend

!macro customUnInstall
  Delete "$INSTDIR\installer-language.txt"
  Delete "$INSTDIR\resources\runtime\ollama-install-mode.txt"
!macroend
