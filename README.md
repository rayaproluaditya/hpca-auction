# HPCA Auction v2
Run: `docker compose up --build`, open http://localhost:4100 (ports and secrets in `.env`).
Demo logins (password `pass`): admin, cap1..cap4, viewer. Anyone can create a profile from the sign-in screen.
Admin flow: sidebar "New auction" -> set purse/timers -> add teams and pick each captain from registered profiles -> add players (from a profile or by stats) -> Start auction.
Captaincy is per auction: whoever is set as a team's captain can bid for that team. Setup locks once the auction starts.
Stats: set `HPCA_STATS_URL` to seed from HPCA; otherwise 12 sample players are seeded into the first demo auction.
Reset data: `docker compose down -v`.
