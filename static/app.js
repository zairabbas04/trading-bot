const fmtNum = (v, d = 2) => {
  const n = Number(v);
  return Number.isFinite(n)
    ? n.toLocaleString(undefined, { minimumFractionDigits: d, maximumFractionDigits: d })
    : '--';
};

const fmtPct = (v, d = 2) => {
  const n = Number(v);
  return Number.isFinite(n) ? `${n.toFixed(d)}%` : '--';
};

const textClass = (n) => !Number.isFinite(n) ? '' : (n > 0 ? 'positive' : (n < 0 ? 'negative' : ''));

let timer = null;
let latestTrades = [];

async function getJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${url} returned ${res.status}`);
  return await res.json();
}

function parseTradeRows(rows) {
  return (rows || []).filter(r => (r.outcome || '').toUpperCase() !== 'OPEN');
}

function summarizeTrades(rows) {
  const trades = parseTradeRows(rows);
  const today = new Date().toISOString().slice(0, 10);
  let pnl = 0, todayPnl = 0, wins = 0, losses = 0;
  const byStrategy = {
    S1_EMA_CROSS: { trades: 0, pnl: 0, wins: 0, losses: 0 },
    S2_MA44_BOUNCE: { trades: 0, pnl: 0, wins: 0, losses: 0 },
  };

  for (const t of trades) {
    const p = Number(t.pnl_usdt || 0);
    pnl += p;
    const closeTime = `${t.close_time || ''}`;
    if (closeTime.startsWith(today)) todayPnl += p;
    if (t.outcome === 'WIN') wins++;
    if (t.outcome === 'LOSS') losses++;
    const s = byStrategy[t.strategy];
    if (s) {
      s.trades++;
      s.pnl += p;
      if (t.outcome === 'WIN') s.wins++;
      if (t.outcome === 'LOSS') s.losses++;
    }
  }

  return { trades, pnl, todayPnl, wins, losses, byStrategy };
}

function updateChip(id, label, state = 'neutral') {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = label;
  el.className = `status-chip ${state}`;
}

function setText(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

function renderSummary(account, stats, summary) {
  const assets = Array.isArray(account.assets) ? account.assets : [];
  const usdt = assets.find(a => a.asset === 'USDT');

  setText('usdtBalance', usdt ? `$${fmtNum(usdt.availableBalance)}` : '--');
  setText('walletSub', usdt ? 'Available in wallet' : 'Waiting for wallet response');

  const totalPnl = document.getElementById('totalPnl');
  totalPnl.textContent = `${summary.pnl >= 0 ? '+' : ''}$${fmtNum(summary.pnl)}`;
  totalPnl.className = `value ${textClass(summary.pnl)}`;

  const todayPnl = document.getElementById('todayPnl');
  todayPnl.textContent = `${summary.todayPnl >= 0 ? '+' : ''}$${fmtNum(summary.todayPnl)}`;
  todayPnl.className = `value ${textClass(summary.todayPnl)}`;

  const total = summary.wins + summary.losses;
  const wr = total ? (summary.wins * 100 / total) : 0;
  const winRate = document.getElementById('winRate');
  winRate.textContent = fmtPct(wr, 1);
  winRate.className = `value ${textClass(wr === 0 ? NaN : wr - 50)}`;
  setText('winSub', `${summary.wins}W / ${summary.losses}L`);

  setText('openCount', `${stats.open_count || 0}`);
  setText('totalTrades', `${summary.trades.length}`);
}

function renderStrategies(stats, summary) {
  const s1 = summary.byStrategy.S1_EMA_CROSS;
  const s2 = summary.byStrategy.S2_MA44_BOUNCE;

  const set = (prefix, data, losses) => {
    const wr = data.trades ? (data.wins * 100 / data.trades) : 0;
    setText(`${prefix}Trades`, `${data.trades}`);
    setText(`${prefix}WinRate`, fmtPct(wr, 0));
    const pnlEl = document.getElementById(`${prefix}Pnl`);
    pnlEl.textContent = `${data.pnl >= 0 ? '+' : ''}${fmtNum(data.pnl)}`;
    pnlEl.className = textClass(data.pnl);
    setText(`${prefix}Losses`, `${losses || 0}`);
  };

  set('s1', s1, stats.consec_losses?.S1_EMA_CROSS ?? stats.consec_losses?.S1 ?? 0);
  set('s2', s2, stats.consec_losses?.S2_MA44_BOUNCE ?? stats.consec_losses?.S2 ?? 0);
}

function renderWallet(account) {
  const assets = (account.assets || []).filter(a => Number(a.walletBalance || a.balance || 0) > 0 || Number(a.availableBalance || 0) > 0);
  const box = document.getElementById('walletAssets');
  setText('walletAssetsCount', `${assets.length} assets`);

  if (!assets.length) {
    box.className = 'wallet-list empty';
    box.textContent = account.msg || 'No wallet data';
    return;
  }

  box.className = 'wallet-list';
  box.innerHTML = assets.map(a => `
    <div class="wallet-row">
      <div class="wallet-asset">
        <span class="asset-dot">${(a.asset || '?').slice(0, 4)}</span>
        <div>
          <strong>${a.asset}</strong>
          <div class="muted">Available ${fmtNum(a.availableBalance || a.balance || 0)}</div>
        </div>
      </div>
      <div><strong>${fmtNum(a.walletBalance || a.balance || 0)}</strong></div>
    </div>`).join('');
}

function renderPositions(positions) {
  const box = document.getElementById('openPositions');
  setText('openPositionsPill', `${positions.length} / 30`);

  if (!positions.length) {
    box.className = 'positions-grid empty';
    box.textContent = 'No open positions';
    return;
  }

  box.className = 'positions-grid';
  box.innerHTML = positions.map(p => {
    const strategyShort = p.strategy?.startsWith('S2') ? 'S2' : 'S1';
    const strategyClass = p.strategy?.startsWith('S2') ? 's2' : 's1';
    const sideClass = (p.direction || '').toLowerCase();
    const symbolLabel = (p.symbol || '--').replace('USDT', '/USDT');
    return `
      <div class="position-card">
        <div class="position-head">
          <div>
            <h3>${symbolLabel}</h3>
            <div class="position-sub">Opened ${p.open_time || '--'}</div>
          </div>
          <div class="tags">
            <span class="tag ${strategyClass}">${strategyShort}</span>
            <span class="tag ${sideClass}">${p.direction || '--'}</span>
          </div>
        </div>
        <div class="kv">
          <div class="k">Entry</div><div>${fmtNum(p.entry_price, 6)}</div>
          <div class="k">Margin</div><div>$${fmtNum(p.margin_usdt)} x ${p.leverage}</div>
          <div class="k">Qty</div><div>${fmtNum(p.quantity, 3)}</div>
          <div class="k">Duration</div><div>${p.duration || '--'}</div>
          <div class="k">Take Profit</div><div class="positive">${fmtNum(p.tp_price, 6)}</div>
          <div class="k">Stop Loss</div><div class="negative">${fmtNum(p.sl_price, 6)}</div>
          <div class="k">Strategy</div><div>${p.strategy || '--'}</div>
        </div>
      </div>`;
  }).join('');
}

function getFilteredTrades(rows) {
  const search = (document.getElementById('tradeSearch')?.value || '').trim().toUpperCase();
  const outcome = document.getElementById('outcomeFilter')?.value || 'ALL';
  const side = document.getElementById('sideFilter')?.value || 'ALL';
  const strategy = document.getElementById('strategyFilter')?.value || 'ALL';

  return parseTradeRows(rows).filter(t => {
    if (search && !(t.symbol || '').toUpperCase().includes(search)) return false;
    if (outcome !== 'ALL' && (t.outcome || '') !== outcome) return false;
    if (side !== 'ALL' && (t.direction || '') !== side) return false;
    if (strategy !== 'ALL' && (t.strategy || '') !== strategy) return false;
    return true;
  }).slice().reverse();
}

function renderTrades(rows) {
  latestTrades = rows || [];
  const trades = getFilteredTrades(latestTrades);
  setText('tradeCountPill', `${trades.length} trades`);
  const body = document.getElementById('tradeRows');

  if (!trades.length) {
    body.innerHTML = '<tr><td colspan="10" class="muted center">No trades match the current filters</td></tr>';
    return;
  }

  body.innerHTML = trades.map(t => {
    const pnl = Number(t.pnl_usdt || 0);
    const pnlPct = Number(t.pnl_pct || 0);
    const outcomeClass = t.outcome === 'WIN' ? 'positive' : (t.outcome === 'LOSS' ? 'negative' : '');
    const strategyShort = t.strategy === 'S2_MA44_BOUNCE' ? 'S2' : (t.strategy === 'S1_EMA_CROSS' ? 'S1' : (t.strategy || '--'));
    return `<tr>
      <td>${t.open_time || '--'}<br><span class="muted">${t.close_time || '--'}</span></td>
      <td><strong>${t.symbol || '--'}</strong></td>
      <td>${strategyShort}</td>
      <td>${t.direction || '--'}</td>
      <td>${fmtNum(t.entry_price, 6)}</td>
      <td>SL ${fmtNum(t.sl_price, 6)}<br><span class="muted">TP ${fmtNum(t.tp_price, 6)}</span></td>
      <td class="${outcomeClass}">${t.outcome || '--'}</td>
      <td class="${textClass(pnl)}">${pnl >= 0 ? '+' : ''}${fmtNum(pnl)}</td>
      <td>${fmtNum(t.fee_usdt || 0)}</td>
      <td class="${textClass(pnlPct)}">${pnlPct >= 0 ? '+' : ''}${fmtPct(pnlPct, 2)}</td>
    </tr>`;
  }).join('');
}

function renderStatus(account, stats, openPositions, trades) {
  const items = [];
  const accountOk = account && !account.code && !account.msg;
  const s1Losses = stats.consec_losses?.S1_EMA_CROSS ?? stats.consec_losses?.S1 ?? 0;
  const s2Losses = stats.consec_losses?.S2_MA44_BOUNCE ?? stats.consec_losses?.S2 ?? 0;

  updateChip('botStateChip', `Bot Live - ${openPositions.length} open`, 'neutral');
  updateChip('apiStateChip', accountOk ? 'Binance API OK' : 'API Needs Attention', accountOk ? 'good' : 'bad');

  items.push(`<li>Binance account API: <strong class="${accountOk ? 'positive' : 'negative'}">${accountOk ? 'OK' : 'Error'}</strong>${account.msg ? ` - ${account.msg}` : ''}</li>`);
  items.push(`<li>Open positions tracker: <strong>${openPositions.length}</strong> active</li>`);
  items.push(`<li>Closed trades loaded: <strong>${parseTradeRows(trades).length}</strong></li>`);
  items.push(`<li>Consecutive losses: <strong>S1=${s1Losses}</strong>, <strong>S2=${s2Losses}</strong></li>`);

  document.getElementById('statusList').innerHTML = items.join('');
}

async function refresh() {
  updateChip('apiStateChip', 'Refreshing...', 'neutral');
  try {
    const [account, stats, openPositions, trades] = await Promise.all([
      getJson('/proxy/fapi/v2/account').catch(() => ({ msg: 'Failed to reach account API' })),
      getJson('/proxy/stats').catch(() => ({ consec_losses: { S1: 0, S2: 0 }, open_count: 0 })),
      getJson('/proxy/open_positions').catch(() => ([])),
      getJson('/proxy/trades').catch(() => ([])),
    ]);

    const safePositions = Array.isArray(openPositions) ? openPositions : [];
    const safeTrades = Array.isArray(trades) ? trades : [];
    const summary = summarizeTrades(safeTrades);
    renderSummary(account, stats, summary);
    renderStrategies(stats, summary);
    renderWallet(account);
    renderPositions(safePositions);
    renderTrades(safeTrades);
    renderStatus(account, stats, safePositions, safeTrades);
    setText('lastUpdated', `Updated ${new Date().toLocaleTimeString()}`);
  } catch (e) {
    console.error(e);
    updateChip('apiStateChip', 'Refresh Failed', 'bad');
  }
}

function applyTimer() {
  if (timer) clearInterval(timer);
  const sec = Number(document.getElementById('refreshSelect').value || 0);
  if (sec > 0) timer = setInterval(refresh, sec * 1000);
}

function bindFilters() {
  ['tradeSearch', 'outcomeFilter', 'sideFilter', 'strategyFilter'].forEach(id => {
    const el = document.getElementById(id);
    if (!el) return;
    el.addEventListener('input', () => renderTrades(latestTrades));
    el.addEventListener('change', () => renderTrades(latestTrades));
  });
}

document.getElementById('refreshBtn').addEventListener('click', refresh);
document.getElementById('refreshSelect').addEventListener('change', applyTimer);
bindFilters();
applyTimer();
refresh();
