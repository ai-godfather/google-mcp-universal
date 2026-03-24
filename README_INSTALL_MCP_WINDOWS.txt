================================================================
  GOOGLE ADS UNIVERSAL MCP — INSTALACJA NA WINDOWS 10
  Krok po kroku dla poczatkujacych
================================================================
  Autor: God_FatherAI (https://x.com/God_FatherAI)
  Repo:  https://github.com/ai-godfather/google-mcp-universal
================================================================


SPIS TRESCI
-----------
  1. Co to jest i jak dziala
  2. Dwa sposoby instalacji (plugin vs MCP standalone)
  3. Wymagania wstepne
  4. SPOSOB A: Instalacja jako plugin (najprostsza)
  5. SPOSOB B: Instalacja jako samodzielny serwer MCP
  6. Konfiguracja konta Google Ads (config.json)
  7. Uzyskanie danych OAuth do Google Ads API
  8. Uruchomienie i testowanie
  9. Rozwiazywanie problemow
  10. FAQ



================================================================
1. CO TO JEST I JAK DZIALA
================================================================

Google Ads Universal MCP to "wtyczka" (plugin), ktora pozwala
Claude Desktop / Claude Code rozmawiac z Twoim kontem Google Ads.

Dziala to tak:

  Claude Desktop  <--stdio-->  Serwer MCP (Python)  <--API-->  Google Ads

- Claude wysyla polecenia do serwera MCP (np. "pokaz kampanie")
- Serwer MCP laczy sie z Google Ads API i zwraca wyniki
- Claude pokazuje Ci wyniki i moze podejmowac dzialania

Plugin zawiera 148 narzedzi:
  - 92 narzedzia core (kampanie, reklamy, slowa kluczowe, Merchant Center)
  - 56 narzedzi batch optimizer (masowa konfiguracja, audyty, optymalizacja)
  - 27 komend slash (/ads-health, /ads-report, /ads-audit, itd.)



================================================================
2. DWA SPOSOBY INSTALACJI
================================================================

SPOSOB A: JAKO PLUGIN (NAJPROSTSZA — zalecana)
  - Wgrywasz plik .plugin do Claude Desktop / Cowork
  - Claude sam startuje serwer MCP w tle
  - Ustawiasz zmienne srodowiskowe w konfiguracji pluginu
  - Najlatwiejszy sposob — 5 minut

SPOSOB B: JAKO SAMODZIELNY SERWER MCP
  - Rozpakowujesz pliki na dysk
  - Recznie edytujesz plik konfiguracyjny Claude Desktop
  - Wiecej kontroli, ale wymaga edycji plikow JSON
  - Dla zaawansowanych — 15-20 minut

Jesli nie wiesz co wybrac — wybierz SPOSOB A (plugin).



================================================================
3. WYMAGANIA WSTEPNE
================================================================

a) Python 3.10 lub nowszy
   - Wejdz na: https://www.python.org/downloads/
   - Pobierz "Download Python 3.12.x" (najnowszy)
   - Przy instalacji KONIECZNIE zaznacz:
     [x] "Add Python to PATH"    <--- TO JEST KRYTYCZNE!
   - Kliknij "Install Now"

   Sprawdzenie czy dziala — otworz CMD (Win+R, wpisz "cmd"):
     python --version
   Powinno wyswietlic: Python 3.12.x

b) Claude Desktop (najnowsza wersja)
   - Wejdz na: https://claude.ai/download
   - Zainstaluj Claude Desktop dla Windows

c) Konto Google Ads z dostepem do API
   - Potrzebujesz: Customer ID, Developer Token, OAuth credentials
   - Szczegoly w rozdziale 7



================================================================
4. SPOSOB A: INSTALACJA JAKO PLUGIN (NAJPROSTSZA)
================================================================

Krok 1: Pobierz plik pluginu
-----------------------------
  Pobierz plik: google-mcp-universal.plugin
  (z GitHub Releases lub od osoby, ktora Ci go udostepnila)


Krok 2: Zainstaluj wymagane biblioteki Python
----------------------------------------------
  Otworz CMD (Win+R -> "cmd" -> Enter) i wpisz:

    pip install fastmcp google-ads python-dotenv pydantic google-api-python-client google-auth httpx

  Jesli "pip" nie dziala, sprobuj:

    python -m pip install fastmcp google-ads python-dotenv pydantic google-api-python-client google-auth httpx

  Poczekaj az wszystko sie zainstaluje (moze trwac 1-2 minuty).


Krok 3: Zainstaluj plugin w Claude Desktop
-------------------------------------------
  Opcja A — przez Cowork:
    - Otworz Claude Desktop -> Cowork
    - Kliknij "Plugins" (ikona wtyczki)
    - Kliknij "Install plugin" lub przeciagnij plik .plugin

  Opcja B — reczna instalacja:
    - Plik .plugin to archiwum ZIP
    - Rozpakuj go do folderu, np.:
        C:\Users\TWOJA_NAZWA\.claude\plugins\google-mcp-universal\
    - Struktura powinna wygladac tak:
        google-mcp-universal\
          .claude-plugin\
            plugin.json
          skills\
            google-mcp-universal\
              google_ads_mcp.py    <-- glowny serwer MCP
              batch_optimizer.py
              ... (reszta plikow .py)
          commands\
            ads-health.md
            ads-report.md
            ... (reszta komend)
          config.example.json
          setup_account.py


Krok 4: Skonfiguruj konto
--------------------------
  Otworz CMD i przejdz do folderu pluginu:

    cd C:\Users\TWOJA_NAZWA\.claude\plugins\google-mcp-universal

  Uruchom kreator konfiguracji:

    python setup_account.py

  Kreator zada kilka pytan (Customer ID, domeny, rynki).
  Utworzy plik config.json z Twoimi danymi.

  ALTERNATYWNIE: Skopiuj config.example.json jako config.json
  i wypelnij recznie (patrz rozdzial 6).


Krok 5: Ustaw zmienne srodowiskowe
------------------------------------
  Plugin potrzebuje zmiennych srodowiskowych z danymi OAuth.
  Mozesz je ustawic na 2 sposoby:

  SPOSOB 1 — Zmienne systemowe Windows:
    - Wcisnij Win+R, wpisz: sysdm.cpl
    - Zakladka "Zaawansowane" -> "Zmienne srodowiskowe"
    - Dodaj nowe zmienne uzytkownika:

      GOOGLE_ADS_DEVELOPER_TOKEN    = twoj_developer_token
      GOOGLE_ADS_CLIENT_ID          = twoj_client_id.apps.googleusercontent.com
      GOOGLE_ADS_CLIENT_SECRET      = GOCSPX-twoj_client_secret
      GOOGLE_ADS_REFRESH_TOKEN      = 1//twoj_refresh_token
      GOOGLE_ADS_CUSTOMER_ID        = 1234567890
      GOOGLE_ADS_LOGIN_CUSTOMER_ID  = 9876543210  (opcjonalnie, MCC ID)

  SPOSOB 2 — Plik .env w folderze pluginu:
    Utworz plik ".env" (bez rozszerzenia) w folderze pluginu:
      C:\Users\TWOJA_NAZWA\.claude\plugins\google-mcp-universal\.env

    Zawartosc pliku .env:
      GOOGLE_ADS_DEVELOPER_TOKEN=twoj_developer_token
      GOOGLE_ADS_CLIENT_ID=twoj_client_id.apps.googleusercontent.com
      GOOGLE_ADS_CLIENT_SECRET=GOCSPX-twoj_client_secret
      GOOGLE_ADS_REFRESH_TOKEN=1//twoj_refresh_token
      GOOGLE_ADS_CUSTOMER_ID=1234567890
      GOOGLE_ADS_LOGIN_CUSTOMER_ID=9876543210


Krok 6: Zrestartuj Claude Desktop
-----------------------------------
  - Zamknij Claude Desktop calkowicie (tray icon -> Quit)
  - Otworz ponownie
  - Plugin powinien sie zaladowac automatycznie
  - Wpisz: /ads-health   — aby sprawdzic czy dziala



================================================================
5. SPOSOB B: INSTALACJA JAKO SAMODZIELNY SERWER MCP
================================================================

Ta metoda daje wiecej kontroli — recznie edytujesz konfiguracje
Claude Desktop zeby wskazywal na serwer MCP.


Krok 1: Rozpakuj pliki
------------------------
  Wybierz folder na dysku, np.:
    C:\MCP\google-ads-universal\

  Rozpakuj plik .plugin (to ZIP) do tego folderu.
  Lub sklonuj repo:
    git clone https://github.com/ai-godfather/google-mcp-universal.git C:\MCP\google-ads-universal


Krok 2: Zainstaluj biblioteki Python
--------------------------------------
  Otworz CMD:
    cd C:\MCP\google-ads-universal
    pip install -r requirements.txt

  Lub recznie:
    pip install fastmcp google-ads python-dotenv pydantic google-api-python-client google-auth httpx


Krok 3: Skonfiguruj konto (config.json)
-----------------------------------------
  cd C:\MCP\google-ads-universal
  python setup_account.py

  Lub skopiuj config.example.json -> config.json i wypelnij recznie.


Krok 4: Utworz plik .env z danymi OAuth
-----------------------------------------
  Utworz plik:
    C:\MCP\google-ads-universal\.env

  Zawartosc:
    GOOGLE_ADS_DEVELOPER_TOKEN=twoj_developer_token
    GOOGLE_ADS_CLIENT_ID=twoj_client_id.apps.googleusercontent.com
    GOOGLE_ADS_CLIENT_SECRET=GOCSPX-twoj_client_secret
    GOOGLE_ADS_REFRESH_TOKEN=1//twoj_refresh_token
    GOOGLE_ADS_CUSTOMER_ID=1234567890
    GOOGLE_ADS_LOGIN_CUSTOMER_ID=9876543210


Krok 5: Edytuj konfiguracje Claude Desktop
--------------------------------------------
  Plik konfiguracyjny Claude Desktop na Windows znajduje sie w:

    %APPDATA%\Claude\claude_desktop_config.json

  Pelna sciezka to np.:
    C:\Users\TWOJA_NAZWA\AppData\Roaming\Claude\claude_desktop_config.json

  Jak go znalezc:
    - Wcisnij Win+R
    - Wpisz: %APPDATA%\Claude
    - Enter — otworzy sie folder
    - Jesli nie ma pliku claude_desktop_config.json — utworz go

  UWAGA: Jesli plik juz istnieje i ma inne serwery MCP,
  dodaj wpis "google-ads" do istniejacego bloku "mcpServers".
  NIE nadpisuj calego pliku!

  Zawartosc pliku (nowy plik lub dodaj do istniejacego):

  {
    "mcpServers": {
      "google-ads": {
        "command": "python",
        "args": [
          "C:\\MCP\\google-ads-universal\\skills\\google-mcp-universal\\google_ads_mcp.py"
        ],
        "env": {
          "GOOGLE_ADS_DEVELOPER_TOKEN": "twoj_developer_token",
          "GOOGLE_ADS_CLIENT_ID": "twoj_client_id.apps.googleusercontent.com",
          "GOOGLE_ADS_CLIENT_SECRET": "GOCSPX-twoj_client_secret",
          "GOOGLE_ADS_REFRESH_TOKEN": "1//twoj_refresh_token",
          "GOOGLE_ADS_CUSTOMER_ID": "1234567890",
          "GOOGLE_ADS_LOGIN_CUSTOMER_ID": "9876543210",
          "GOOGLE_ADS_PLUGIN_CONFIG": "C:\\MCP\\google-ads-universal\\config.json"
        }
      }
    }
  }

  WAZNE:
  - Uzyj PODWOJNYCH backslashy w sciezkach: C:\\MCP\\... (nie C:\MCP\...)
  - Lub uzyj slashy: C:/MCP/google-ads-universal/...
  - "command" to "python" (nie "python3" — na Windows python3 czesto nie dziala)
  - GOOGLE_ADS_PLUGIN_CONFIG wskazuje na Twoj config.json


Krok 6: Zrestartuj Claude Desktop
-----------------------------------
  - Zamknij Claude Desktop calkowicie (tray icon -> Quit)
  - Otworz ponownie
  - Otworz nowa rozmowe
  - Wpisz: /ads-health



================================================================
6. KONFIGURACJA KONTA GOOGLE ADS (config.json)
================================================================

Plik config.json zawiera dane Twojego konta. Oto najwazniejsze pola:

  "account": {
    "customer_id": "1234567890",     <-- ID konta Google Ads (10 cyfr)
    "mcc_id": "",                    <-- ID konta MCC (jesli masz)
    "company_name": "Twoja Firma",
    "brand_name": "Twoj Brand",
    "industry": "e-commerce",
    "developer_token": "..."         <-- token z Google Ads API Center
  }

  "merchant_center": {
    "merchant_ids": {
      "US": "111111111",             <-- ID Merchant Center per kraj
      "DE": "222222222"
    }
  }

  "domains": {
    "US": "us.twojsklep.com",        <-- domeny sklepow per kraj
    "DE": "de.twojsklep.com"
  }

  "markets": ["US", "DE"],           <-- lista aktywnych rynkow

  "ai_copy": {
    "openai_api_key": "sk-...",      <-- klucz OpenAI (opcjonalny)
    "model": "gpt-4o"               <-- model do generowania reklam
  }

Najszybszy sposob: uruchom "python setup_account.py" — kreator
przeprowadzi Cie przez cala konfiguracje.



================================================================
7. UZYSKANIE DANYCH OAUTH DO GOOGLE ADS API
================================================================

Zeby plugin mogl laczyc sie z Google Ads, potrzebujesz 4 rzeczy:

  1. Developer Token
  2. OAuth Client ID + Client Secret
  3. Refresh Token
  4. Customer ID

Oto jak je uzyskac:


--- 7a. Developer Token ---

  1. Zaloguj sie na: https://ads.google.com
  2. Kliknij ikone klucza (Narzedzia i ustawienia)
  3. W sekcji "Setup" -> "API Center"
  4. Jesli nie widzisz API Center — musisz miec konto MCC
     (Manager Account): https://ads.google.com/home/tools/manager-accounts/
  5. Skopiuj "Developer Token"
  6. Poczatkowo token jest w trybie "Test" — dziala tylko z kontami
     testowymi. Aby uzywac z prawdziwym kontem, zloz wniosek
     o "Basic Access" lub "Standard Access".


--- 7b. OAuth Client ID i Client Secret ---

  1. Wejdz na: https://console.cloud.google.com
  2. Utworz nowy projekt lub wybierz istniejacy
  3. Wlacz API:
     - APIs & Services -> Library
     - Wyszukaj "Google Ads API" -> Enable
     - Wyszukaj "Content API for Shopping" -> Enable (opcjonalnie)
  4. Utworz credentials:
     - APIs & Services -> Credentials
     - Create Credentials -> OAuth Client ID
     - Application type: "Desktop app"
     - Nazwa: np. "Google Ads MCP"
     - Kliknij "Create"
  5. Skopiuj "Client ID" i "Client Secret"
  6. Skonfiguruj OAuth consent screen:
     - APIs & Services -> OAuth consent screen
     - Wypelnij podstawowe dane
     - Dodaj scope: https://www.googleapis.com/auth/adwords
     - Dodaj scope: https://www.googleapis.com/auth/content
       (opcjonalnie, dla Merchant Center)


--- 7c. Refresh Token ---

  To jest najtrudniejszy krok. Musisz wykonac "OAuth flow":

  SPOSOB 1 — Przez Google OAuth Playground (najlatwiejszy):

    1. Wejdz na: https://developers.google.com/oauthplayground/
    2. Kliknij ikone zebatki (Settings) w prawym gornym rogu
    3. Zaznacz "Use your own OAuth credentials"
    4. Wpisz swoj Client ID i Client Secret
    5. W lewym panelu znajdz i zaznacz:
       - Google Ads API v18 -> https://www.googleapis.com/auth/adwords
    6. Kliknij "Authorize APIs"
    7. Zaloguj sie swoim kontem Google (tym samym co Google Ads)
    8. Kliknij "Exchange authorization code for tokens"
    9. Skopiuj "Refresh token" z odpowiedzi

  SPOSOB 2 — Przez skrypt Python:

    pip install google-auth-oauthlib

    Utworz plik get_refresh_token.py:

      from google_auth_oauthlib.flow import InstalledAppFlow
      flow = InstalledAppFlow.from_client_config(
          {"installed": {
              "client_id": "TWOJ_CLIENT_ID",
              "client_secret": "TWOJ_CLIENT_SECRET",
              "auth_uri": "https://accounts.google.com/o/oauth2/auth",
              "token_uri": "https://oauth2.googleapis.com/token"
          }},
          scopes=["https://www.googleapis.com/auth/adwords",
                  "https://www.googleapis.com/auth/content"]
      )
      creds = flow.run_local_server(port=8080)
      print(f"Refresh Token: {creds.refresh_token}")

    Uruchom:
      python get_refresh_token.py

    Otworzy sie przegladarka — zaloguj sie i zatwierdz.
    Refresh token pojawi sie w konsoli.


--- 7d. Customer ID ---

  1. Zaloguj sie na: https://ads.google.com
  2. Customer ID jest widoczny w prawym gornym rogu
     Format: 123-456-7890
  3. W konfiguracji wpisz BEZ myslnikow: 1234567890



================================================================
8. URUCHOMIENIE I TESTOWANIE
================================================================

Po instalacji i konfiguracji:

  1. Zrestartuj Claude Desktop (zamknij -> otworz)
  2. Otworz nowa rozmowe
  3. Wpisz jedno z ponizszych:

     /ads-health          — szybki test polaczenia
     /ads-report          — raport wydajnosci kampanii
     /ads-quota           — sprawdz limit API

  4. Jesli dziala — zobaczysz dane z Twojego konta Google Ads
  5. Jesli nie — patrz rozdzial 9 (Rozwiazywanie problemow)

Przykladowe pytania do Claude:
  - "Pokaz moje kampanie Google Ads"
  - "Jaki mam budzet na kampanie w Polsce?"
  - "Pokaz slowa kluczowe z najwyzszym ROAS"
  - "Wstrzymaj kampanie z ROAS ponizej 1.0"
  - "Pokaz produkty w Merchant Center"



================================================================
9. ROZWIAZYWANIE PROBLEMOW
================================================================

PROBLEM: "python" nie jest rozpoznawane jako polecenie
  -> Python nie jest w PATH.
  -> Reinstaluj Python z zaznaczonym "Add to PATH"
  -> Lub uzyj pelnej sciezki: C:\Python312\python.exe

PROBLEM: "pip" nie dziala
  -> Uzyj: python -m pip install ...
  -> Lub: py -m pip install ...

PROBLEM: Claude nie widzi serwera MCP
  -> Sprawdz czy plik claude_desktop_config.json jest poprawny
  -> Sprawdz sciezki — na Windows uzywaj \\ lub /
  -> Zrestartuj Claude Desktop CALKOWICIE (Quit, nie minimalizuj)
  -> Sprawdz czy w "command" masz "python" (nie "python3")

PROBLEM: Blad "ModuleNotFoundError: No module named 'fastmcp'"
  -> Zainstaluj ponownie: pip install fastmcp
  -> Upewnij sie ze uzywasz tego samego Pythona co w konfiguracji
  -> Sprawdz: python -c "import fastmcp; print('OK')"

PROBLEM: Blad polaczenia z Google Ads API
  -> Sprawdz czy Customer ID jest poprawny (10 cyfr, bez myslnikow)
  -> Sprawdz czy Developer Token jest aktywny (nie "Test")
  -> Sprawdz czy Refresh Token nie wygasl
  -> Sprawdz czy OAuth Client ma wlaczony scope "adwords"

PROBLEM: "google.auth.exceptions.RefreshError"
  -> Refresh Token wygasl lub zostal odwolany
  -> Wygeneruj nowy Refresh Token (patrz rozdzial 7c)

PROBLEM: Serwer MCP startuje ale nie ma narzedzi
  -> Sprawdz sciezke do google_ads_mcp.py w konfiguracji
  -> Poprawna sciezka wskazuje na:
       skills\google-mcp-universal\google_ads_mcp.py
     (NIE na folder glowny pluginu)

PROBLEM: config.json nie jest ladowany
  -> Sprawdz czy zmienna GOOGLE_ADS_PLUGIN_CONFIG wskazuje
     na poprawna sciezke do config.json
  -> Lub umiesc config.json w tym samym folderze co google_ads_mcp.py



================================================================
10. FAQ
================================================================

P: Czy musze placic za ten plugin?
O: Nie. Plugin jest darmowy (licencja MIT).
   Potrzebujesz konta Google Ads (z budzetem reklamowym)
   i opcjonalnie klucza OpenAI (do generowania reklam AI).

P: Czy plugin moze cos zepsuc w moim koncie Google Ads?
O: Plugin zawiera zabezpieczenia (guardrails), ale operacje
   takie jak wstrzymywanie kampanii sa nieodwracalne.
   Zawsze testuj na koncie testowym najpierw.

P: Czy moge uzywac z wieloma kontami Google Ads?
O: Tak. Uzyj konta MCC (Manager) i skonfiguruj
   GOOGLE_ADS_LOGIN_CUSTOMER_ID na ID konta MCC.

P: Czy potrzebuje klucza OpenAI?
O: Nie jest wymagany. Bez niego plugin uzywa szablonow
   do generowania tresci reklam (RSA). Z kluczem OpenAI
   tresci beda generowane przez AI (lepsza jakosc).

P: Czy to dziala z Claude Code (CLI)?
O: Tak. Claude Code rowniez obsluguje serwery MCP.
   Konfiguracja jest w pliku:
     ~/.claude/settings.json (macOS/Linux)
     %USERPROFILE%\.claude\settings.json (Windows)

P: Czy musze miec "Basic Access" do Google Ads API?
O: Dla kont testowych wystarczy "Test Access".
   Dla prawdziwych kont reklamowych potrzebujesz
   "Basic Access" lub "Standard Access".

P: Gdzie moge zglosic blad lub poprosic o pomoc?
O: https://github.com/ai-godfather/google-mcp-universal/issues


================================================================
  Dziekujemy za uzywanie Google Ads Universal MCP!
  God_FatherAI — https://x.com/God_FatherAI
================================================================
