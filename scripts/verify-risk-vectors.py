"""Check design risk vectors with Decimal; this is not the production risk engine."""
from decimal import Decimal as D, getcontext, ROUND_CEILING, ROUND_FLOOR
import json
from pathlib import Path
getcontext().prec=100
Q=D('0.00000001')
def up(x): return x.quantize(Q,rounding=ROUND_CEILING)
def down(x): return x.quantize(Q,rounding=ROUND_FLOOR)
def fmt(x): return f'{x:.8f}'
def amount(x): return fmt(up(x))
def check(c):
 i=c['input']; f=c['formula']; z=D(0)
 v=lambda k:D(i[k])
 if f=='SPOT_PARTIAL_FILL':
  orig=up(v('original_quantity')*v('buy_cap')); debit=up(v('fill_quantity')*v('fill_price')); q=v('original_quantity')-v('fill_quantity'); rem=up(q*v('buy_cap'))+v('fee_tail')
  return dict(zip(['original_reserve','actual_debit','remaining_quantity','remaining_reserve','released_upper_bound','debit_plus_reserve'],map(fmt,[orig,debit,q,rem,orig-debit-rem,debit+rem])))
 if f=='SPOT_RECEIVED_FEE':
  fees=[up(v('received_amount')*v(k)) for k in ['standard_rate','tax_rate','special_rate']]
  return {'standard_fee':fmt(fees[0]),'tax_fee':fmt(fees[1]),'special_fee':fmt(fees[2]),'total_fee':fmt(sum(fees)),'net_received':fmt(down(v('received_amount')-sum(fees))),'existing_base_inventory_reserve':fmt(z)}
 if f=='SPOT_THIRD_ASSET_FEE_ESTIMATE':
  fees=[up(v('received_quote')*v(k)) for k in ['standard_rate','tax_rate','special_rate']]
  bnb=sum(up(x/v('bnb_quote_low')) for x in fees)
  return {'fee_quote_upper':fmt(sum(fees)),'bnb_fee_estimate':fmt(bnb),'fallback_net_received_quote':fmt(down(v('received_quote')-sum(fees))),'exclusive_fee_pool_reserve':fmt(v('bnb_fee_pool_balance')),'bnb_available_for_other_actions':fmt(z)}
 if f=='SPOT_AVAILABLE':
  total=v('free0')+v('locked0')+v('net_cashflow_after_baseline')
  local=total-v('nontrade_locked')-sum(D(o['remaining_reserve']) for o in i['orders'])
  cap=v('free0')+v('credits_after_baseline')-v('other_debits')-sum(max(z,D(o['actual_debit'])+D(o['remaining_reserve'])-D(o['coverage'])) for o in i['orders'])
  return dict(zip(['total_assets','local_free','venue_cap','available'],map(fmt,[total,local,cap,max(z,min(local,cap))])))
 if f=='ONE_WAY_ENVELOPE':
  lo=v('position')-v('pending_sell'); hi=v('position')+v('pending_buy'); cap=abs(v('position'))+v('pending_buy')+v('pending_sell')
  return dict(zip(['position_min','position_max','max_abs_position','capital_quantity','capital_notional','all_filled_position'],map(fmt,[lo,hi,max(abs(lo),abs(hi)),cap,up(cap*v('stress_price_high')),v('position')+v('pending_buy')-v('pending_sell')])))
 if f=='HEDGE_ENVELOPE':
  long=v('long')+v('open_long'); short=v('short')+v('open_short'); lo=max(z,v('long')-v('close_long'))-short; hi=long-max(z,v('short')-v('close_short')); cap=sum(v(k) for k in ['long','short','open_long','open_short','close_long','close_short'])
  return dict(zip(['long_max','short_max','net_min','net_max','capital_quantity','capital_notional'],map(fmt,[long,short,lo,hi,cap,up(cap*v('stress_price_high'))])))
 if f=='MM_TIER_AND_UPPER':
  tiers=[]; mm=[]; upper=[]
  for n0 in i['notionals']:
   n=D(n0)
   j=next(j for j,b in enumerate(i['brackets']) if D(b['floor'])<=n and (n<D(b['cap']) or (j==len(i['brackets'])-1 and n==D(b['cap']))))
   b=i['brackets'][j]; tiers.append(str(j+1)); mm.append(amount(max(z,n*D(b['rate'])-D(b['cum'])))); upper.append(amount(n*max(D(x['rate']) for x in i['brackets'][:j+1])))
  return {'tiers':tiers,'tier_maintenance':mm,'upper_maintenance':upper}
 if f=='BRACKET_QUALIFICATION':
  valid=i['bracket_interpretation']!='unverified'
  return {'decision':'ACCEPT' if valid else 'REJECT','reason':None if valid else 'BRACKET_SEMANTICS_UNVERIFIED','multiply_returned_caps':False}
 if f=='COLLATERAL_VALUE':
  vals=[]; invalid=z
  for a in i['assets']:
   bal=D(a['balance']); val=down(bal*D(a['valuation_low'])*D(a['eligible_ratio'])) if bal>=0 and a['ratio_valid'] else (-up(-bal*D(a['valuation_high'])) if bal<0 else z)
   vals.append(val)
   if a['asset']!=i['invalidate_asset'] or bal<0: invalid+=val
  return {'components':list(map(fmt,vals)),'collateral_value':fmt(sum(vals)),'collateral_after_invalidation':fmt(invalid)}
 if f=='POTENTIAL_ORDER_SUM':
  rs=[up(D(o['quantity'])*D(o['buy_cap'])) for o in i['orders']]
  return {'reserves':list(map(fmt,rs)),'total_reserve':fmt(sum(rs)),'release_on_timeout':fmt(z),'new_risk_decision':'REJECT' if i['has_unknown'] else 'ACCEPT','reason':'UNKNOWN_EXPOSURE' if i['has_unknown'] else None}
 if f=='SERIAL_RESERVATION':
  avail=v('available'); decisions=[]; values=[]
  for r in i['requests']:
   ok=D(r['amount'])<=avail; decisions.append('ACCEPT' if ok else 'REJECT'); avail-=D(r['amount']) if ok else z; values.append(fmt(avail))
  return {'decisions':decisions,'available_after_each':values,'second_reason':'INSUFFICIENT_ASSET' if decisions[1]=='REJECT' else None}
 if f=='REDUCE_ALLOCATION':
  before=max(z,v('actual_long')-sum(map(D,i['existing_allocations']))); alloc=min(before,v('requested_quantity')) if i['allow_partial_reduce'] else (v('requested_quantity') if before>=v('requested_quantity') else z)
  return {'reducible_before':fmt(before),'allocated_quantity':fmt(alloc),'reducible_after':fmt(before-alloc),'position_side':'BOTH','side':'SELL','reduce_only':True}
 if f=='FIXED_POINT_BOUNDARIES':
  n=int(i['quantity_atoms'])*int(i['price_atoms']); div=10**(i['quantity_scale']+i['price_scale']-i['money_scale']); floor=n//div; ceil=-(-n//div); sn=int(i['signed_numerator']); sd=int(i['signed_denominator']); overflow=not(-(2**127)<=int(i['overflow_left'])*int(i['overflow_right'])<=2**127-1)
  return {'notional_floor_atoms':str(floor),'notional_ceil_atoms':str(ceil),'notional_floor':fmt(D(floor).scaleb(-i['money_scale'])),'notional_ceil':fmt(D(ceil).scaleb(-i['money_scale'])),'signed_floor':str(sn//sd),'signed_ceil':str(-(-sn//sd)),'overflow_decision':'REJECT' if overflow else 'ACCEPT','overflow_reason':'ARITHMETIC_OVERFLOW' if overflow else None}
 if f=='FUTURES_CAPITAL':
  qty=v('position_long')+v('buy_quantity')+v('sell_quantity'); n=up(qty*v('price_high')); im=up(n/v('effective_leverage')); mm=up(n*v('maintenance_rate_upper')); pos=up(max(z,v('position_long')*(v('entry')-v('price_low')))); order=up(v('buy_quantity')*max(z,v('buy_cap')-v('price_low')))+up(v('sell_quantity')*max(z,v('price_high')-v('sell_floor')))
  fee=up(up(v('buy_quantity')*v('buy_cap'))*v('execution_fee_rate'))+up(up(v('sell_quantity')*max(v('price_high'),v('sell_floor')))*v('execution_fee_rate')); exit_qty=max(abs(v('position_long')-v('sell_quantity')),abs(v('position_long')+v('buy_quantity'))); ex=up(exit_qty*v('price_high')*v('exit_fee_rate')); fund=up(n*v('funding_rate_abs_upper')*v('funding_settlements'))
  cap=max(im,mm+v('maintenance_addon'))+pos+order+fee+ex+fund+v('pending_outbound')+v('capital_buffer')
  return dict(zip(['capital_quantity','capital_notional','initial_charge','maintenance_upper','position_loss','order_loss','execution_fee','exit_fee','funding_reserve','capital','local_headroom','exit_quantity'],map(fmt,[qty,n,im,mm,pos,order,fee,ex,fund,cap,v('collateral_value')-cap,exit_qty])))
 if f=='STRATEGY_ACCOUNT_NETTING':
  positions=list(map(D,i['strategy_positions'])); before=sum(positions); after=before-v('strategy_a_sell'); gross=sum(abs(p) for p in positions)*v('price'); nb=abs(before)*v('price'); na=abs(after)*v('price')
  return {'account_position_before':fmt(before),'strategy_gross_notional_before':fmt(gross),'account_net_notional_before':fmt(nb),'account_position_after':fmt(after),'account_net_notional_after':fmt(na),'risk_class':'ACCOUNT_INCREASING' if na>nb else 'ACCOUNT_NONINCREASING'}
 if f=='VENUE_SHADOW_CAP':
  cap=max(z,v('available0')-sum(map(D,i['uncovered_holds']))-v('uncovered_other_debits')-v('market_additional_hold')); ok=v('new_capital_delta')<=cap
  return {'shadow_available':fmt(cap),'decision':'ACCEPT' if ok else 'REJECT','reason':None if ok else 'VENUE_AVAILABLE_CAP'}
 if f=='FEE_ROUNDING_TAIL':
  agg=up(v('received_amount')*v('fee_rate')); tail=v('maximum_remaining_fills')*v('fee_asset_quantum')
  return {'aggregate_fee_ceil':fmt(agg),'rounding_tail':fmt(tail),'fee_reserve':fmt(agg+tail)}
 if f=='SIGNED_POSITION_LOSS':
  q=v('position'); stress=v('price_low') if q>=0 else v('price_high'); loss=up(max(z,q*(v('entry')-stress)))
  return {'absolute_quantity':fmt(abs(q)),'stress_price':fmt(stress),'position_loss':fmt(loss)}
 if f=='POST_FILL_EXIT_FEE':
  assert v('position')==0 and v('sell_quantity')==0 and v('fill_quantity')==v('buy_quantity'), 'this regression models a flat account followed by a full buy fill'
  q=v('position'); hi=q+v('buy_quantity'); lo=q-v('sell_quantity'); maxq=max(abs(lo),abs(hi)); capitalq=abs(q)+v('buy_quantity')+v('sell_quantity'); n=up(capitalq*v('price_high')); im=up(n/v('effective_leverage')); mm=up(n*v('maintenance_rate_upper'))+v('maintenance_addon'); pos=z; order=up(v('buy_quantity')*max(z,v('buy_cap')-v('price_low'))); fee=up(v('buy_quantity')*v('buy_cap')*v('execution_fee_rate')); exitfee=up(maxq*v('price_high')*v('exit_fee_rate')); other=v('funding_reserve')+v('pending_outbound')+v('capital_buffer'); cap=max(im,mm)+pos+order+fee+exitfee+other; head=v('wallet_before')-cap
  afterq=q+v('fill_quantity'); afterwallet=v('wallet_before')-up(v('fill_quantity')*v('fill_price')*v('execution_fee_rate')); afterexit=up(abs(afterq)*v('price_high')*v('exit_fee_rate')); after_n=up(abs(afterq)*v('price_high')); after_im=up(after_n/v('effective_leverage')); after_mm=up(after_n*v('maintenance_rate_upper'))+v('maintenance_addon'); afterloss=up(max(z,afterq*(v('fill_price')-v('price_low')))); aftercap=max(after_im,after_mm)+afterloss+afterexit+other; adequate_wallet=cap-up(v('fill_quantity')*v('fill_price')*v('execution_fee_rate'))
  return {'max_abs_position':fmt(maxq),'initial_charge_before':fmt(im),'position_loss_before':fmt(pos),'order_loss_before':fmt(order),'execution_fee_before':fmt(fee),'exit_fee_before':fmt(exitfee),'capital_before':fmt(cap),'local_headroom_before':fmt(head),'decision':'REJECT' if head<0 else 'ACCEPT','reason':'INSUFFICIENT_COLLATERAL' if head<0 else None,'hypothetical_position_after':fmt(afterq),'hypothetical_wallet_after':fmt(afterwallet),'hypothetical_exit_fee_after':fmt(afterexit),'hypothetical_capital_after':fmt(aftercap),'hypothetical_local_headroom_after':fmt(afterwallet-aftercap),'minimum_wallet_before':fmt(cap),'adequately_funded_wallet_after':fmt(adequate_wallet),'adequately_funded_headroom_after':fmt(adequate_wallet-aftercap)}
 if f=='LINEAR_SETTLEMENT_RECONCILE':
  assert i['funding_mode']=='NONE' and not i['complete_account_facts_at_timer'] and i['complete_account_facts_after_reconcile'] and i['unresolved_orders_after_reconcile']==0
  q=v('position_signed'); reserve=up(abs(q)*v('stress_price_high')*v('settlement_fee_rate_upper'))+v('settlement_rounding_buffer'); pnl=down(q*(v('actual_settlement_price')-v('entry_price'))); fee=up(abs(q)*v('actual_settlement_price')*v('actual_fee_rate_for_reference')); net=pnl-fee; actual=sum(D(e['amount']) for e in i['actual_income_components']); actual_fee=-sum(D(e['amount']) for e in i['actual_income_components'] if e['canonical_type']=='SETTLEMENT_FEE'); wallet=v('wallet_before')+actual
  return {'funding_reserve':fmt(z),'fee_reserve_before':fmt(reserve),'reference_pnl':fmt(pnl),'reference_fee':fmt(fee),'reference_net':fmt(net),'actual_net_income':fmt(actual),'expected_wallet_from_actual_income':fmt(wallet),'wallet_reconciliation_difference':fmt(v('observed_wallet_after')-wallet),'reference_vs_actual_difference':fmt(actual-net),'position_after_timer':fmt(q),'fee_reserve_after_timer':fmt(reserve),'released_on_timer':fmt(z),'estimated_price_income_booked':fmt(z),'state_after_timer':'AWAITING_SETTLEMENT','position_after_reconcile':fmt(v('actual_position_after_reconcile')),'remaining_fee_reserve_released':fmt(max(z,reserve-actual_fee)),'state_after_reconcile':'SETTLED_RECONCILED','automatically_roll_position':False}
 raise RuntimeError(f)
x=json.loads((Path(__file__).resolve().parents[1] / 'specs/contracts/risk-vectors-v1.json').read_text())
errors=[]; field_count=0
for c in x['cases']:
 actual=check(c); field_count+=len(c['expected'])
 if actual!=c['expected']: errors.append({'id':c['id'],'expected':c['expected'],'actual':actual})
print(json.dumps({'cases':len(x['cases']),'expected_fields':field_count,'failures':errors},ensure_ascii=False,indent=2))
raise SystemExit(bool(errors))
