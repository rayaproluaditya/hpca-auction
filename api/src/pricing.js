// Formula and thresholds come from auction settings and the pricing_rules table, never from code.
exports.scoreOf=(s,w)=>Math.min(100,Math.round(s.runs*w.runs+s.avg*w.avg+s.sr*w.sr+s.wickets*w.wickets));
exports.categorize=(score,rules)=>rules.find(r=>score>=+r.min_score);
