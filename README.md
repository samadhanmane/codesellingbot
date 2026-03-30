# sellingbot (Telegram code seller)

## What it does
- Shows stock counts for `500 / 1000 / 2000 / 4000`
- User flow: QR -> `Payment Done` -> UTR -> payment screenshot -> admin approve/decline
- On admin accept: one available code is claimed atomically and sent to the user, then removed from stock
- On decline: nothing is sent and the code is not taken
- User can view last orders (history)
- Admin can upload codes via CSV (prefix-based routing: `svi->500`, `svc->1000`, `svd->2000`, `svh->4000`)
- Admin can export payment/order history as CSV

## Setup
1. Install Node.js (18+ recommended).
2. Copy `.env.example` to `.env` and fill values:
   - `BOT_TOKEN`
   - `ADMIN_CHAT_ID`
   - `MONGODB_URI`
   - `REQUIRED_CHANNEL` (fallback; admin can configure channels inside the bot)
   - `PAYMENT_QR_PHOTO_FILE_ID` or `PAYMENT_QR_IMAGE_URL` (fallback; admin can upload QR inside the bot)
3. Install dependencies:
   - `npm install`
4. Run:
   - `npm start`

## Admin actions
- `/start_admin` then tap:
  - **Configure Required Channel(s)** to enter 1+ channel links/usernames (one per line)
  - **Upload Payment QR (photo)** to upload/change the QR image used for users
  - **Export payments (CSV)** for payment/order history CSV
- Upload a CSV document to the bot from the admin account to import codes.

## User actions
- `/start` (you must join the required channel first)
- Tap a category button to start an order.
- If you get stuck, start over with `/start`.

