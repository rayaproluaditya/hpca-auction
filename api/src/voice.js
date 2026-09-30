// Commentary only. Receives committed events, returns text. No DB access, no engine calls.
// Swap this for an LLM/TTS provider adapter later; keep the input as verified events.
module.exports=e=>({PLAYER_PRESENTED:`Up next, ${e.player}. Base price ${e.base} coins.`,BID:`${e.team} bids ${e.bid}.`,PLAYER_SOLD:`Sold! ${e.player} goes to ${e.team} for ${e.amount} coins.`,PLAYER_UNSOLD:`${e.player} goes unsold.`,AUCTION_PAUSED:'The auction is paused.',AUCTION_LIVE:'We are live.',BIDDING_REOPENED:'Bidding is reopened.'}[e.action]||null);
