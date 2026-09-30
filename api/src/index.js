const express=require('express'),http=require('http'),{Server}=require('socket.io'),{Pool}=require('pg'),jwt=require('jsonwebtoken'),crypto=require('crypto'),fs=require('fs'),path=require('path');
const {scoreOf,categorize}=require('./pricing'),{fetchPlayers}=require('./stats'),announce=require('./voice');
const pool=new Pool({connectionString:process.env.DATABASE_URL}),SECRET=process.env.JWT_SECRET||'dev-secret';
const hash=p=>crypto.scryptSync(p,'hpca',32).toString('hex');
// Player state machine: the only legal transitions.
const T={UPCOMING:['BIDDING','UNSOLD'],BIDDING:['PENDING_CONFIRMATION','UNSOLD','UPCOMING'],PENDING_CONFIRMATION:['SOLD','BIDDING','UNSOLD'],SOLD:['REOPENED'],REOPENED:['BIDDING']};
class E extends Error{constructor(m,s=400){super(m);this.status=s}}
const app=express(),srv=http.createServer(app),io=new Server(srv);
app.use(express.json());app.use(express.static(path.join(__dirname,'../public')));
app.get('/health',(q,r)=>r.json({ok:true}));

// Every change runs in one transaction, bumps the version, then broadcasts the full snapshot.
async function tx(aid,fn){const c=await pool.connect();c.evs=[];
 try{await c.query('begin');const out=await fn(c);await c.query('update auctions set version=version+1 where id=$1',[aid]);await c.query('commit');
  c.evs.forEach(e=>{const t=announce(e);if(t)io.to('a'+aid).emit('announce',{text:t})});await push(aid);return out}
 catch(e){await c.query('rollback');throw e}finally{c.release()}}
const log=async(c,aid,ap,action,u,prev,next,reason,team)=>{await c.query('insert into auction_events(auction_id,ap_id,team_id,user_id,role,action,prev,next,reason) values($1,$2,$3,$4,$5,$6,$7,$8,$9)',[aid,ap,team||null,u?u.id:null,u?u.role:'SYSTEM',action,prev,next,reason||null]);c.evs.push({action,...next})};
async function move(c,ap,to,sets={}){if(!(T[ap.status]||[]).includes(to))throw new E(`Invalid transition ${ap.status} to ${to}`);const k=Object.keys(sets);
 return (await c.query(`update auction_players set status=$1${k.map((x,i)=>`,${x}=$${i+3}`).join('')} where id=$2 returning *`,[to,ap.id,...k.map(x=>sets[x])])).rows[0]}

async function bid(aid,u,apId,amount,reqId){
 return tx(aid,async c=>{
  const a=(await c.query('select * from auctions where id=$1',[aid])).rows[0];
  const ap=(await c.query('select * from auction_players where id=$1 and auction_id=$2 for update',[apId,aid])).rows[0]; // row lock orders simultaneous bids
  if(!ap)throw new E('No such player',404);
  if(a.status!=='LIVE')throw new E('The auction is not live');
  if(ap.status!=='BIDDING'||new Date(ap.end_time)<=Date.now())throw new E('Bidding is closed for this player');
  const tm=(await c.query('select * from teams where auction_id=$1 and captain_id=$2 for update',[aid,u.id])).rows[0];if(!tm)throw new E('Only captains can bid',403);
  const min=ap.current_bid==null?ap.base_price:ap.current_bid+ap.increment;
  if(amount<min||(amount-ap.base_price)%ap.increment)throw new E(`Bid ${min} or a further step of ${ap.increment}`);
  if(ap.team_id===tm.id)throw new E('You already hold the highest bid');
  if(amount>tm.purse)throw new E('Insufficient purse');
  const n=(await c.query("select count(*)::int n from auction_players where team_id=$1 and status='SOLD'",[tm.id])).rows[0].n;
  if(n>=a.settings.squad_max)throw new E('Your squad is full');
  const seq=ap.seq+1;
  const ins=await c.query('insert into bids(ap_id,team_id,user_id,amount,seq,request_id) values($1,$2,$3,$4,$5,$6) on conflict do nothing returning id',[ap.id,tm.id,u.id,amount,seq,reqId]);
  if(!ins.rowCount)return{duplicate:true};
  const end=new Date(Math.max(+new Date(ap.end_time),Date.now()+a.settings.reset*1000));
  await c.query('update auction_players set current_bid=$1,team_id=$2,seq=$3,end_time=$4 where id=$5',[amount,tm.id,seq,end,ap.id]);
  await log(c,aid,ap.id,'BID',u,{bid:ap.current_bid},{bid:amount,player:ap.name,team:tm.name},null,tm.id);
  return{ok:true,seq}})}

const cur=(c,aid)=>c.query("select * from auction_players where auction_id=$1 and status in('BIDDING','PENDING_CONFIRMATION') for update",[aid]).then(r=>r.rows[0]);
async function admin(aid,u,act,reason,pid){return tx(aid,async c=>{
 const a=(await c.query('select * from auctions where id=$1 for update',[aid])).rows[0],S=a.settings,now=Date.now(),p=await cur(c,aid);
 const need=(x,m)=>{if(!x)throw new E(m)};
 const setA=async s=>{await c.query('update auctions set status=$1 where id=$2',[s,aid]);await log(c,aid,null,'AUCTION_'+s,u,{status:a.status},{status:s},reason)};
 const L=(ap,action,prev,next,team)=>log(c,aid,ap.id,action,u,prev,next,reason,team);
 switch(act){
  case 'start':{need(['DRAFT','SCHEDULED'].includes(a.status),'Already started');
   const tms=(await c.query('select captain_id from teams where auction_id=$1',[aid])).rows;need(tms.length>=2,'Add at least two teams');need(tms.every(t=>t.captain_id),'Every team needs a captain');
   need((await c.query('select count(*)::int n from auction_players where auction_id=$1',[aid])).rows[0].n>0,'Add players to the pool');return setA('LIVE')}
  case 'pause':need(a.status==='LIVE','Auction is not live');if(p&&p.status==='BIDDING')await c.query('update auction_players set paused_ms=$1 where id=$2',[Math.max(0,new Date(p.end_time)-now),p.id]);return setA('PAUSED');
  case 'resume':need(a.status==='PAUSED','Auction is not paused');if(p&&p.status==='BIDDING')await c.query('update auction_players set end_time=$1 where id=$2',[new Date(now+(p.paused_ms||0)),p.id]);return setA('LIVE');
  case 'end':need(a.status!=='COMPLETED','Already ended');return setA('COMPLETED');
  case 'next':{need(a.status==='LIVE','Start the auction first');need(!p,'Finish the current player first');
   const n=(await c.query("select * from auction_players where auction_id=$1 and status='UPCOMING' order by pos limit 1 for update",[aid])).rows[0];need(n,'No players left');
   await move(c,n,'BIDDING',{end_time:new Date(now+S.timer*1000),current_bid:null,team_id:null});
   return L(n,'PLAYER_PRESENTED',{status:'UPCOMING'},{status:'BIDDING',player:n.name,base:n.base_price,category:n.category})}
  case 'confirm':{need(p&&p.status==='PENDING_CONFIRMATION','Nothing is waiting for confirmation');need(p.team_id,'No bids on this player');
   await c.query('update teams set purse=purse-$1 where id=$2',[p.current_bid,p.team_id]);await move(c,p,'SOLD',{sold_price:p.current_bid});
   const t=(await c.query('select name from teams where id=$1',[p.team_id])).rows[0];
   return L(p,'PLAYER_SOLD',{status:p.status},{status:'SOLD',player:p.name,team:t.name,amount:p.current_bid},p.team_id)}
  case 'reopen':need(p&&p.status==='PENDING_CONFIRMATION','Only a pending player can be reopened');
   await move(c,p,'BIDDING',{end_time:new Date(now+S.reset*1000)});return L(p,'BIDDING_REOPENED',{status:p.status},{status:'BIDDING',player:p.name});
  case 'unsold':need(p,'No active player');await move(c,p,'UNSOLD',{current_bid:null,team_id:null});return L(p,'PLAYER_UNSOLD',{status:p.status},{status:'UNSOLD',player:p.name});
  case 'skip':{need(p&&p.status==='BIDDING','No active player');const m=(await c.query('select max(pos) m from auction_players where auction_id=$1',[aid])).rows[0].m;
   await c.query('delete from bids where ap_id=$1',[p.id]);await move(c,p,'UPCOMING',{pos:m+1,current_bid:null,team_id:null,end_time:null});return L(p,'PLAYER_SKIPPED',{status:p.status},{status:'UPCOMING',player:p.name})}
  case 'undo':{need(p&&p.status==='BIDDING','No active player');const last=(await c.query('select id from bids where ap_id=$1 order by seq desc limit 1',[p.id])).rows[0];need(last,'No bids to undo');
   await c.query('delete from bids where id=$1',[last.id]);const pr=(await c.query('select amount,team_id from bids where ap_id=$1 order by seq desc limit 1',[p.id])).rows[0];
   await c.query('update auction_players set current_bid=$1,team_id=$2 where id=$3',[pr?pr.amount:null,pr?pr.team_id:null,p.id]);return L(p,'BID_UNDONE',{bid:p.current_bid},{bid:pr?pr.amount:null,player:p.name})}
  case 'reverse':{need(reason,'A reason is required to reverse a sale');need(!p,'Finish the current player first');
   const s=(await c.query("select * from auction_players where id=$1 and auction_id=$2 and status='SOLD' for update",[pid,aid])).rows[0];need(s,'That player is not sold');
   await c.query('update teams set purse=purse+$1 where id=$2',[s.sold_price,s.team_id]);const r=await move(c,s,'REOPENED',{sold_price:null,current_bid:null,team_id:null});
   await move(c,r,'BIDDING',{end_time:new Date(now+S.timer*1000)});return L(s,'SALE_REVERSED',{status:'SOLD',team:s.team_id,price:s.sold_price},{status:'BIDDING',player:s.name})}
  default:throw new E('Unknown action',404)}})}

// Server-authoritative timer: expired bidding moves to admin confirmation. It never finalizes a sale.
setInterval(async()=>{try{const r=await pool.query("select id,auction_id from auction_players where status='BIDDING' and end_time<now() and auction_id in(select id from auctions where status='LIVE')");
 for(const x of r.rows)await tx(x.auction_id,async c=>{const p=(await c.query("select * from auction_players where id=$1 and status='BIDDING' and end_time<now() for update",[x.id])).rows[0];if(!p)return;
  const to=p.current_bid?'PENDING_CONFIRMATION':'UNSOLD';await move(c,p,to);await log(c,x.auction_id,p.id,to==='UNSOLD'?'PLAYER_UNSOLD':'TIMER_EXPIRED',null,{status:'BIDDING'},{status:to,player:p.name})})}catch(e){console.error(e.message)}},1000);

async function snap(aid){const q=(s,p)=>pool.query(s,p).then(r=>r.rows);const [a]=await q('select * from auctions where id=$1',[aid]);if(!a)throw new E('Auction not found',404);
 const teams=await q("select t.id,t.name,t.purse,t.captain_id,coalesce(u.display_name,u.username) captain,(select count(*)::int from auction_players p where p.team_id=t.id and p.status='SOLD') squad from teams t left join users u on u.id=t.captain_id where t.auction_id=$1 order by t.id",[aid]);
 const players=await q('select id,name,role,stats,score,category,base_price,increment,status,current_bid,team_id,end_time,paused_ms,sold_price from auction_players where auction_id=$1 order by pos',[aid]);
 const bids=await q("select b.amount,t.name team from bids b join teams t on t.id=b.team_id where b.ap_id=(select id from auction_players where auction_id=$1 and status in('BIDDING','PENDING_CONFIRMATION') limit 1) order by b.seq desc limit 12",[aid]);
 const events=await q('select action,role,created_at,reason,next from auction_events where auction_id=$1 order by id desc limit 15',[aid]);
 return{auction:a,teams,players,bids,events,server_ts:Date.now()}}
const push=async aid=>io.to('a'+aid).emit('state',await snap(aid));

const DEF={purse:1000,timer:20,reset:10,squad_max:4,weights:{runs:.06,avg:.8,sr:.15,wickets:1.5}};
const pn=(v,d)=>Number.isFinite(+v)&&+v>0?Math.round(+v):d,fl=v=>Number.isFinite(+v)?+v:0;
// Admin setup: create and edit auctions, teams, captains and players. Locked once the auction starts.
async function setup(u,k,b){const aid=+b.aid;
 if(k==='auction'&&!aid){const r=await pool.query("insert into auctions(name,status,settings) values($1,'DRAFT',$2) returning id",[String(b.name||'New auction').slice(0,80),DEF]);io.emit('list');return{id:r.rows[0].id}}
 if(k==='auction-del'){const a=(await pool.query('select status from auctions where id=$1',[aid])).rows[0];if(!a||a.status!=='DRAFT')throw new E('Only draft auctions can be deleted');
  for(const t of['auction_events','auction_players','teams'])await pool.query(`delete from ${t} where auction_id=$1`,[aid]);await pool.query('delete from auctions where id=$1',[aid]);io.emit('list');return{ok:true}}
 const out=await tx(aid,async c=>{
  const a=(await c.query('select * from auctions where id=$1 for update',[aid])).rows[0];if(!a)throw new E('Auction not found',404);
  if(!['DRAFT','SCHEDULED'].includes(a.status))throw new E('Setup is locked once the auction has started');
  const L=(x,y)=>log(c,aid,null,x,u,null,y,b.reason);
  if(k==='auction'){const S={...a.settings,purse:pn(b.purse,a.settings.purse),timer:pn(b.timer,a.settings.timer),reset:pn(b.reset,a.settings.reset),squad_max:pn(b.squad_max,a.settings.squad_max)};
   await c.query('update auctions set name=$1,settings=$2 where id=$3',[String(b.name||a.name).slice(0,80),S,aid]);await c.query('update teams set purse=$1 where auction_id=$2',[S.purse,aid]);return L('SETUP_AUCTION_EDITED',{name:b.name})}
  if(k==='team'){const name=String(b.name||'').trim().slice(0,60);if(!name)throw new E('Team name is required');const cap=b.captain_id?+b.captain_id:null;
   if(cap&&(await c.query('select 1 from teams where auction_id=$1 and captain_id=$2 and id is distinct from $3',[aid,cap,b.id||null])).rowCount)throw new E('That person already captains another team');
   if(b.id)await c.query('update teams set name=$1,captain_id=$2 where id=$3 and auction_id=$4',[name,cap,b.id,aid]);else await c.query('insert into teams(name,auction_id,captain_id,purse) values($1,$2,$3,$4)',[name,aid,cap,a.settings.purse]);
   return L('SETUP_TEAM_SAVED',{team:name,captain_id:cap})}
  if(k==='team-del'){await c.query('delete from teams where id=$1 and auction_id=$2',[b.id,aid]);return L('SETUP_TEAM_REMOVED',{id:b.id})}
  if(k==='player'){const name=String(b.name||'').trim().slice(0,60);if(!name)throw new E('Player name is required');const x=b.stats||{};
   const st={matches:fl(x.matches),runs:fl(x.runs),avg:fl(x.avg),sr:fl(x.sr),wickets:fl(x.wickets),econ:fl(x.econ)};
   const rules=(await c.query('select * from pricing_rules order by min_score desc')).rows,score=scoreOf(st,a.settings.weights||DEF.weights),rl=categorize(score,rules),role=b.role||'Batter';
   if(b.id)await c.query('update auction_players set name=$1,role=$2,stats=$3,score=$4,category=$5,base_price=$6,increment=$7 where id=$8 and auction_id=$9',[name,role,st,score,rl.category,rl.base_price,rl.increment,b.id,aid]);
   else{const m=(await c.query('select coalesce(max(pos),-1)+1 p from auction_players where auction_id=$1',[aid])).rows[0].p;
    await c.query('insert into auction_players(auction_id,name,role,stats,score,category,base_price,increment,pos,player_ref) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)',[aid,name,role,st,score,rl.category,rl.base_price,rl.increment,m,b.user_id?'u'+b.user_id:null])}
   return L('SETUP_PLAYER_SAVED',{player:name,category:rl.category})}
  if(k==='player-del'){await c.query('delete from auction_players where id=$1 and auction_id=$2',[b.id,aid]);return L('SETUP_PLAYER_REMOVED',{id:b.id})}
  throw new E('Unknown setup action',404)});
 io.emit('list');return out}
const auth=(q,r,n)=>{try{q.u=jwt.verify((q.headers.authorization||'').slice(7),SECRET);n()}catch{r.status(401).json({error:'Sign in again'})}};
const wrap=f=>async(q,r)=>{try{const o=await f(q);r.json(o===undefined?{ok:true}:o)}catch(e){if(!e.status)console.error(e);r.status(e.status||500).json({error:e.status?e.message:'Server error'})}};
const session=u=>({token:jwt.sign({id:u.id,role:u.role,name:u.username},SECRET,{expiresIn:'12h'}),user:{id:u.id,name:u.display_name||u.username,role:u.role}});
app.post('/api/register',wrap(async q=>{const{username,password,display_name,bat_role}=q.body;
 if(!/^[a-z0-9_]{3,20}$/i.test(username||''))throw new E('Username must be 3 to 20 letters, numbers or underscores');if((password||'').length<4)throw new E('Password needs at least 4 characters');
 try{const u=(await pool.query("insert into users(username,pass,role,display_name,bat_role) values($1,$2,'PLAYER',$3,$4) returning *",[username.toLowerCase(),hash(password),String(display_name||username).slice(0,60),bat_role||'Batter'])).rows[0];io.emit('list');return session(u)}
 catch(e){if(e.code==='23505')throw new E('That username is taken',409);throw e}}));
app.post('/api/login',wrap(async q=>{const u=(await pool.query('select * from users where username=$1',[q.body.username])).rows[0];if(!u||u.pass!==hash(q.body.password||''))throw new E('Wrong username or password',401);
 return session(u)}));
app.get('/api/state',auth,wrap(q=>snap(+q.query.aid)));
app.get('/api/auctions',auth,wrap(()=>pool.query('select a.id,a.name,a.status,(select count(*)::int from teams t where t.auction_id=a.id) teams,(select count(*)::int from auction_players p where p.auction_id=a.id) players from auctions a order by a.id desc').then(r=>r.rows)));
app.get('/api/users',auth,wrap(q=>{if(q.u.role!=='ADMIN')throw new E('Admins only',403);return pool.query("select id,username,coalesce(display_name,username) name,bat_role from users where role<>'ADMIN' order by username").then(r=>r.rows)}));
app.post('/api/setup/:k',auth,wrap(q=>{if(q.u.role!=='ADMIN')throw new E('Admins only',403);return setup(q.u,q.params.k,q.body)}));
const hits={};
app.post('/api/bid',auth,wrap(q=>{const k=q.u.id,n=Date.now();hits[k]=(hits[k]||[]).filter(t=>n-t<1000);if(hits[k].length>=5)throw new E('Too many bids, slow down',429);hits[k].push(n);
 const{player_id,amount,request_id}=q.body;if(!Number.isInteger(amount)||!request_id)throw new E('Invalid bid');return bid(+q.body.aid,q.u,player_id,amount,String(request_id))}));
app.post('/api/admin/:act',auth,wrap(q=>{if(q.u.role!=='ADMIN')throw new E('Admins only',403);return admin(+q.body.aid,q.u,q.params.act,q.body.reason,q.body.player_id)}));
io.use((s,n)=>{try{s.u=jwt.verify(s.handshake.auth.token,SECRET);n()}catch{n(new Error('auth'))}});
io.on('connection',s=>{s.on('join',async aid=>{try{aid=+aid;if(!Number.isInteger(aid))return;if(s.aid)s.leave('a'+s.aid);s.aid=aid;s.join('a'+aid);s.emit('state',await snap(aid))}catch(e){}})}); // clients re-join on reconnect to resync

async function init(){await pool.query(fs.readFileSync(path.join(__dirname,'schema.sql'),'utf8'));if((await pool.query('select 1 from auctions')).rowCount)return;
 const S={purse:1000,timer:20,reset:10,squad_max:4,weights:{runs:.06,avg:.8,sr:.15,wickets:1.5}};
 await pool.query("insert into pricing_rules(category,min_score,base_price,increment) values('A',75,500,50),('B',50,250,25),('C',0,100,10)");
 const rules=(await pool.query('select * from pricing_rules order by min_score desc')).rows;
 const aid=(await pool.query("insert into auctions(name,status,settings) values('HPCA Premier League Auction 2026','SCHEDULED',$1) returning id",[S])).rows[0].id;
 await pool.query("insert into users(username,pass,role) values('admin',$1,'ADMIN'),('viewer',$1,'VIEWER')",[hash('pass')]);
 const names=['Hyderabad Hawks','Deccan Kings','Charminar Chargers','Golconda Giants'];
 for(let i=0;i<names.length;i++){const t=(await pool.query('insert into teams(name,auction_id,purse) values($1,$2,$3) returning id',[names[i],aid,S.purse])).rows[0].id;
  const u=(await pool.query("insert into users(username,pass,role,team_id) values($1,$2,'CAPTAIN',$3) returning id",['cap'+(i+1),hash('pass'),t])).rows[0].id;
  await pool.query('update teams set captain_id=$1 where id=$2',[u,t])}
 const ps=await fetchPlayers();
 for(let i=0;i<ps.length;i++){const p=ps[i],score=scoreOf(p,S.weights),r=categorize(score,rules);
  await pool.query('insert into auction_players(auction_id,player_ref,name,role,stats,score,category,base_price,increment,pos) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)',[aid,p.ref,p.name,p.role,{matches:p.matches,runs:p.runs,avg:p.avg,sr:p.sr,wickets:p.wickets,econ:p.econ},score,r.category,r.base_price,r.increment,i])}}
init().then(()=>srv.listen(4000,()=>console.log('HPCA Auction on :4000'))).catch(e=>{console.error(e);process.exit(1)});
