================================================================
  GOOGLE ADS MCP UNIVERSAL — INSTALACJA NA WINDOWS (krok po kroku)
================================================================
  Wersja 1.1.0. Pelna instrukcja (macOS / Linux / Windows, po angielsku): INSTALL.md
  Ten plik jest napisany tak, zeby asystent AI (Claude Code / Claude Desktop) mogl
  przeprowadzic instalacje za uzytkownika. Kroki oznaczone [UZYTKOWNIK] wykonuje czlowiek.
================================================================

SPIS TRESCI
  0. Co sie instaluje i gdzie sa klucze
  1. Wymagania
  2. Kod i biblioteki (wirtualne srodowisko .venv)
  3. Dane dostepowe Google (developer token, OAuth, refresh token)
  4. Ustawienia konta (config.json)
  5. Test na zywo
  6. Podlaczenie do Claude Code / Claude Desktop
  7. Pierwsze uzycie i aktualizacja
  8. Rozwiazywanie problemow

================================================================
0. CO SIE INSTALUJE I GDZIE SA KLUCZE
================================================================
  - Serwer MCP (Python): skills\google-mcp-universal\google_ads_mcp.py — 186 narzedzi
    Google Ads / Merchant Center / PageSpeed. Wymagany.
  - Plugin Claude Code (51 komend /ads-... + 2 skille). Opcjonalny, tylko Claude Code.

  W folderze instalacji (dalej: FOLDER):
  - .venv\        wirtualne srodowisko Pythona z bibliotekami
  - .env          SEKRETY: developer token, OAuth client ID/secret, refresh token (nie commitowac)
  - config.json   ustawienia konta: customer ID, MCC, rynki, domeny, ID Merchant Center (bez sekretow)

  Serwer sam czyta FOLDER\.env i FOLDER\config.json — konfiguracja Claude nie potrzebuje kluczy.

  Zasady dla asystenta: nigdy nie prosic o wklejenie kluczy do czatu, nie wyswietlac zawartosci
  .env, a zmiany na koncie wykonywac dopiero po wyraznej zgodzie uzytkownika (narzedzia domyslnie
  dzialaja w trybie podgladu: dry_run / validate_only).

================================================================
1. WYMAGANIA
================================================================
  - Git:            git --version          (instalacja: https://git-scm.com/download/win)
  - Python 3.12–3.14: py -0                (instalacja: https://www.python.org — zaznacz "Add to PATH")
  - Claude Code (claude --version) i/lub Claude Desktop
  - [UZYTKOWNIK] konto Google z dostepem do konta Google Ads (najlepiej przez konto menedzera MCC),
    opcjonalnie dostep do Merchant Center.

================================================================
2. KOD I BIBLIOTEKI (PowerShell)
================================================================
  git clone https://github.com/ai-godfather/google-mcp-universal.git $HOME\google-mcp-universal
  cd $HOME\google-mcp-universal
  py -3.13 -m venv .venv
  .venv\Scripts\python -m pip install -r requirements.txt
  .venv\Scripts\python verify_install.py

  Oczekiwane: "[OK] MCP tools/list — 186 tools". Linie [FAIL] o config.json i kluczach sa
  normalne do czasu wykonania krokow 3–4.

  WAZNE: zawsze uzywaj .venv\Scripts\python.exe — systemowy Python nie ma bibliotek.

================================================================
3. DANE DOSTEPOWE GOOGLE
================================================================
  3.1 [UZYTKOWNIK] Projekt Google Cloud: https://console.cloud.google.com/
      Wlacz API: "Google Ads API", "Content API for Shopping" (Merchant Center),
      opcjonalnie "PageSpeed Insights API".

  3.2 [UZYTKOWNIK] Ekran zgody OAuth (OAuth consent screen):
      - typ External (albo Internal w Google Workspace)
      - zakresy: https://www.googleapis.com/auth/adwords oraz https://www.googleapis.com/auth/content
      - dodaj swoje konto jako testowe, potem "Publish app" -> "In production"
        (w trybie Testing Google uniewaznia refresh token po 7 dniach!)

  3.3 [UZYTKOWNIK] Klient OAuth: Credentials -> Create credentials -> OAuth client ID ->
      typ "Desktop app" -> Download JSON. Asystentowi podaj tylko SCIEZKE do pliku.

  3.4 [UZYTKOWNIK] Developer token: Google Ads (konto MCC) -> Narzedzia -> API Center.
      Dostep "Test" dziala tylko z kontami testowymi — dla prawdziwego konta potrzebny "Basic".
      Wpisz token do FOLDER\.env:
        copy .env.example .env
        notepad .env        ->  GOOGLE_ADS_DEVELOPER_TOKEN=...

  3.5 Refresh token (uruchamia asystent, loguje sie [UZYTKOWNIK]):
        .venv\Scripts\python generate_refresh_token.py --client-secrets $HOME\Downloads\client_secret_XXXX.json
      Otworzy sie przegladarka — zaloguj sie kontem Google z dostepem do Google Ads (i Merchant Center)
      i zaakceptuj. Skrypt zapisze client ID, secret i refresh token do .env (nie wyswietla ich).
      Opcje: --no-browser (wypisuje link), --ads-only (bez Merchant Center).

================================================================
4. USTAWIENIA KONTA (config.json)
================================================================
  Uzytkownik podaje asystentowi wartosci (to nie sa sekrety):
    .venv\Scripts\python setup_account.py --customer-id 1234567890 --mcc-id 9876543210 `
        --markets PL,DE --domain PL=pl.example.com --domain DE=de.example.com `
        --merchant PL=111111111 --brand "Twoja marka" --company "Twoja firma"
    .venv\Scripts\python setup_account.py --validate

  Albo interaktywnie: .venv\Scripts\python setup_account.py
  --mcc-id pomin, jesli logujesz sie bezposrednio do konta (bez konta menedzera).

================================================================
5. TEST NA ZYWO
================================================================
  .venv\Scripts\python verify_install.py --live

  Oczekiwane na koncu:
    [OK] server sees its setup — status=ready ...
    [OK] Google Ads live query — {"id": "...", "descriptive_name": "...", "currency_code": "..."}
    RESULT: ALL CHECKS PASSED
  Zapytanie jest tylko do odczytu. Przy bledzie skrypt pokazuje podpowiedz (patrz punkt 8).

================================================================
6. PODLACZENIE DO CLAUDE
================================================================
  6.1 Claude Code (PowerShell, w FOLDER):
    claude mcp add --scope user --transport stdio google-ads -- "$PWD\.venv\Scripts\python.exe" "$PWD\skills\google-mcp-universal\google_ads_mcp.py"
    claude mcp list          ->  google-ads ... ✔ Connected

  6.2 Claude Desktop: Ustawienia -> Developer -> Edit Config
      (plik %APPDATA%\Claude\claude_desktop_config.json), dodaj i zapisz:
    {
      "mcpServers": {
        "google-ads": {
          "command": "C:\\Users\\NAZWA\\google-mcp-universal\\.venv\\Scripts\\python.exe",
          "args": ["C:\\Users\\NAZWA\\google-mcp-universal\\skills\\google-mcp-universal\\google_ads_mcp.py"]
        }
      }
    }
      Zachowaj inne serwery w "mcpServers". Potem CALKOWICIE zamknij Claude Desktop
      (ikona w zasobniku -> Quit) i uruchom ponownie. Blok "env" nie jest potrzebny.

  6.3 Komendy i skille (plugin Claude Code, opcjonalnie):
    claude plugin marketplace add ai-godfather/google-mcp-universal
    claude plugin install google-mcp-universal@ai-godfather
      Komendy maja prefiks, np. /google-mcp-universal:ads-health. Plugin nie uruchamia
      serwera MCP — krok 6.1 jest nadal potrzebny.

================================================================
7. PIERWSZE UZYCIE I AKTUALIZACJA
================================================================
  W Claude:
    1. "Wywolaj google_ads_setup_status"  -> status: ready
    2. "Uruchom GAQL: SELECT campaign.id, campaign.name, campaign.status FROM campaign LIMIT 10"
    3. Z pluginem: /google-mcp-universal:ads-health

  Aktualizacja:
    cd $HOME\google-mcp-universal
    git pull
    .venv\Scripts\python -m pip install -r requirements.txt
    .venv\Scripts\python verify_install.py --live
    (potem nowa sesja Claude Code albo restart Claude Desktop;
     plugin: claude plugin marketplace update ai-godfather; claude plugin update google-mcp-universal@ai-godfather)

================================================================
8. ROZWIAZYWANIE PROBLEMOW
================================================================
  Serwer nie startuje / "server exited" / ModuleNotFoundError
    -> konfiguracja Claude wskazuje zly Python; uzyj .venv\Scripts\python.exe
  missing_required w google_ads_setup_status
    -> brak .env w FOLDER albo puste wartosci (krok 3)
  invalid_grant
    -> refresh token wygasl (ekran zgody w trybie Testing = 7 dni) albo odebrano dostep:
       opublikuj ekran zgody (3.2) i uruchom generate_refresh_token.py ponownie
  invalid_client / unauthorized_client
    -> client ID/secret nie pasuja do tokenu: wygeneruj ponownie z wlasciwym JSON-em (3.3–3.5)
  DEVELOPER_TOKEN_NOT_APPROVED
    -> token ma dostep Test; zloz wniosek o Basic access albo uzyj konta testowego
  USER_PERMISSION_DENIED
    -> dostep przez konto menedzera: ustaw --mcc-id (albo GOOGLE_ADS_LOGIN_CUSTOMER_ID w .env)
  CUSTOMER_NOT_FOUND
    -> zly customer ID (10 cyfr, bez myslnikow)
  Merchant Center 403
    -> token bez zakresu content (uzyto --ads-only) albo brak dostepu do MC
  Claude Desktop nie widzi narzedzi
    -> blad skladni JSON (podwojne \\ w sciezkach) albo brak pelnego restartu;
       logi: %APPDATA%\Claude\logs

  Najszybsza diagnoza: .venv\Scripts\python verify_install.py --live — pierwsza linia [FAIL]
  mowi, co poprawic.
================================================================
