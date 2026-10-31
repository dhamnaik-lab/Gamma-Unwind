GAMMA-UNWIND 0.8.9 - options open-interest scanner with a local dashboard and Telegram alerts
=================================================================================================

What it does
  Marks support and resistance zones from daily candles, picks out-of-the-money option contracts
  near those zones and watches them live during market hours. It runs one strategy on three
  lists (Breakdown, Breakout and Carry; the Carry list measures the OI fall its own way). Every
  5 minutes it checks five things on each watched contract: OI falling from its peak, a premium
  jump, volume, a breakout from a quiet range, and the stock agreeing. When all five hold on one
  candle it sends a TRADE ALERT to YOUR Telegram group: the premium, a stop loss and four
  targets, then one message at each target reached and one when a stop or the expiry-eve exit
  ends the trade. An alert from the Carry list is tagged CARRY STRIKE.
  A trade has no fixed holding time: it runs until its stop loss, its last target or the exit on
  the trading day before the contract's expiry. Rows close to an alert (NEAR ENTRY, WATCH) are
  shown on the dashboard only.
  The dashboard runs on your own computer.

  Alerts are information only. They are not investment advice and not a recommendation to trade.
  Nothing is ever ordered for you: the program has no order function at all.

The full guide
  docs\GAMMA-UNWIND-Setup-Guide.docx  (step by step, with pictures, per broker)

Windows 10 / 11 (64-bit)
  1. Right-click the ZIP > Properties > tick "Unblock" > OK. Then extract it to C:\GAMMA
     (a short path; do not run it from inside the ZIP).
     If extraction stops with 'WinError 206' / 'The filename or extension is too long', extract to a
     short folder such as C:\GAMMA.
  2. Fill in .env BEFORE the first start. No .env yet? Double-click "Check Setup.cmd" and answer Y
     to create it from .env.example (or copy .env.example to .env yourself). Open .env with
     Notepad and fill in:
       - the Telegram block (bot token from @BotFather, your standard group id),
       - BROKER= your broker, and ONLY that broker's block (its keys; the daily token can wait).
     Save it.
  3. Double-click "Check Setup.cmd". Fix anything it reports and run it again until it is clean.
     A missing daily token is fine here: you log in on the Broker login page in step 5.
  4. Double-click "Start GAMMA.cmd". A tray icon appears near the clock.
  5. Tray icon > "Open dashboard" (http://127.0.0.1:8760), then log in on the Broker login
     page. The dashboard does not open by itself: open it from the tray menu whenever you need it.
  Stop: tray icon > Quit, or double-click "Stop GAMMA.cmd".
  Update: double-click "Update GAMMA.cmd" (outside market hours; see "Updating" below).

  Windows files
    Start GAMMA.cmd / Stop GAMMA.cmd / Check Setup.cmd   the launchers
    Update GAMMA.cmd                                     updates this folder to the newest version
    python\                                              the bundled Python (do not move or edit)

  Changed .env later? It takes effect on Stop, then Start (tray icon > Stop service > Start
  service). A token entered on the Broker login page takes effect within seconds, no restart
  needed.

Linux / macOS
  macOS: macOS 12 (Monterey) or newer. One ZIP per chip (Apple menu > About This Mac shows it):
    Apple Silicon (M1 or newer)   GAMMA-UNWIND-client-0.8.9-macos-arm64.zip
    Intel                         GAMMA-UNWIND-client-0.8.9-macos-x86_64.zip
    The Mac ZIP carries its own Python (python/): nothing is downloaded or installed, and a
    Python on your Mac (any version, or none) is neither used nor changed.
  Linux (not tested by us): needs Python 3.13 exactly (3.14 is not supported yet).
  1. Extract the ZIP into a NEW folder, open a terminal in that folder.
  2. ./setup.sh        (macOS: checks this Mac and clears the download flag of the folder;
                        Linux: creates .venv and installs the libraries; both create .env)
     Fill in .env as in Windows step 2 above.
  3. ./check.sh        (the setup check; fix anything it reports)
  4. ./start.sh        (dashboard: http://127.0.0.1:8760)   Stop: ./stop.sh
  5. Open the dashboard and log in on the Broker login page.
  Update: ./update.sh  (outside market hours; see "Updating" below)
  macOS, something does not work? Run ./mac_check.sh and send us its whole output.
  macOS, coming from 0.8.1 or older? Do not run ./update.sh in the old folder: extract the new
  ZIP into a new folder, copy .env and the data folder over from the old one (guide 10.5).

  Linux / macOS files
    setup.sh / start.sh / stop.sh / check.sh   the launchers
    update.sh                                  updates this folder to the newest version
    mac_check.sh, python/                      macOS only: the Mac check, the bundled Python

Your daily broker token
  Most brokers issue a new access token every day. Log in on the dashboard's Broker login page,
  or paste it into .env (both work; the program checks the token before saving it). The program
  never logs in to your broker account by itself.
  Logged in after 09:20? The day's 09:20 list was not built yet: tray icon > "Run 09:20 now".

Updating
  The update launcher (listed in your system's section) asks first, then downloads the newest
  version, checks every file against its published checksum, stops GAMMA-UNWIND for about a
  minute, swaps the program files, starts it again and prints the version it reports.
  Never touched: .env, data\, logs\ (your settings, history and logs stay as they are).
  config\strategy.yaml is replaced by the new version's copy; your old copy is kept next to it
  as config\strategy.yaml.bak-<old version>. Changed a setting? Re-apply it from that file.
  config\v1_targets.yaml (your four targets) is replaced by the standard targets; your old copy
  is kept in data\update\backup-<old version>\config\. Changed them? Set them again in the new file.
  The previous version is kept in data\update\backup-<old version>\.
  It ends with a code: 0 updated / 1 nothing to do (already current) / 2 the download did not
  verify, nothing changed / 3 no network or no releases address, nothing changed / 4 GAMMA-UNWIND
  did not stop (port in use), nothing changed / 5 this version needs the full ZIP (a new bundled
  Python or library list): download it and follow the guide / 6 the update failed and the
  previous version was restored and started again.

Also in the guide (chapter numbers as in the docx)
  17 Updating                     the update launcher, step by step, and what each end code means
  18 Dashboard on another device  add GAMMA_LAN=1 and GAMMA_LAN_PASSWORD=<your password> to .env:
                                  the dashboard opens on a phone or tablet on the same Wi-Fi, behind
                                  a login. The Broker login page stays on the PC.
                                  Traffic on your home Wi-Fi is plain HTTP: use it at home only.
  19 Keep the PC awake            while the program runs, the tray keeps the PC awake 08:50-15:50 on
                                  trading days; tray icon > "Wake PC at 08:55" adds a Mon-Fri wake
                                  task so a sleeping PC is up for the 09:00 card and the 09:20 list.
  20 The 09:00 readiness card     one Telegram card every trading morning: READY (silent), or
                                  NOT READY with the first thing to fix; a 09:10 all-clear follows
                                  once it is fixed.
  21 TELEGRAM_TOPIC_ID            for a group with Topics (a forum): the topic the alerts go to.
                                  Blank = the General topic. Check Setup warns when it is needed.

Privacy
  .env holds your secrets. Keep it private and never send it to anyone, including us.
  Nothing leaves your computer except requests to your broker, messages to your own Telegram bot
  and two small daily downloads of public NSE files (the F&O ban list and the results calendar),
  plus, only when you run the update, a download of the newest version from the releases site.

Files (both systems; the launchers are listed in each system's section above)
  .env.example                                         template for .env
  docs\GAMMA-UNWIND-Setup-Guide.docx                   the full guide
  data\  logs\                                         created on first start
  MANIFEST.json                                        list of every file with its checksum
