(function(){'use strict';
const TAB_MAP={resumo:'/admin/leve/resumo',escalas:'/admin/leve/escala',cooperados:'/admin/leve/cooperados',lancamentos:'/admin/leve/lancamentos'};
const PATH_MAP={'/admin/avaliacoes':'/admin/leve/avaliacoes','/admin/documentos':'/admin/leve/documentos','/admin/tabelas':'/admin/leve/tabelas','/admin/avisos':'/admin/leve/avisos'};
function isLegacyUrl(u){return u&&u.searchParams&&u.searchParams.get('legacy')==='1'}
function cleanOld(){document.querySelectorAll('.sidebar,.admin-v6-topbar,.admin-topbar').forEach(el=>el.style.setProperty('display','none','important'));document.querySelectorAll('a').forEach(a=>{const txt=(a.textContent||'').trim().toLowerCase();if(txt==='sistemas'||txt==='sistema'){const wrap=a.closest('li,.nav-item,.admin-v6-group');if(wrap&&wrap!==a)wrap.remove();else a.remove()}})}
function legacy(path){return path+(path.includes('?')?'&':'?')+'legacy=1'}
function topbar(){
 if(document.querySelector('.alv8-top'))return;
 const header=document.createElement('header');header.className='alv8-top';header.innerHTML=`
 <a class="alv8-brand" href="/admin/leve/resumo"><span class="alv8-brand-icon"><i class="bi bi-building-gear"></i></span><span class="alv8-brand-copy"><small>PAINEL COOPEX</small><strong>Administração Central</strong></span></a>
 <nav class="alv8-nav">
  <a class="alv8-direct" href="/admin/leve/resumo"><i class="bi bi-speedometer2"></i> Resumo</a>
  <div class="alv8-group"><button type="button"><i class="bi bi-cash-stack"></i> Financeiro <i class="bi bi-chevron-down"></i></button><div class="alv8-menu">
   <a href="/admin/leve/lancamentos"><i class="bi bi-journal-text"></i> Lançamentos</a><a href="${legacy('/admin?tab=receitas')}"><i class="bi bi-credit-card"></i> Receitas Coop</a><a href="${legacy('/admin?tab=despesas')}"><i class="bi bi-cart-dash"></i> Despesas Coop</a><a href="${legacy('/admin?tab=coop_receitas')}"><i class="bi bi-wallet2"></i> Receitas Cooperados</a><a href="${legacy('/admin?tab=coop_despesas')}"><i class="bi bi-bag-dash"></i> Despesas Cooperados</a><a href="${legacy('/admin?tab=beneficios')}"><i class="bi bi-heart-pulse"></i> Benefícios</a>
  </div></div>
  <div class="alv8-group"><button type="button"><i class="bi bi-people"></i> Cadastros <i class="bi bi-chevron-down"></i></button><div class="alv8-menu"><a href="/admin/leve/cooperados"><i class="bi bi-person-badge"></i> Cooperados</a><a href="${legacy('/admin?tab=restaurantes')}"><i class="bi bi-shop-window"></i> Estabelecimentos</a><a href="${legacy('/admin/documentos')}"><i class="bi bi-folder2"></i> Documentos</a><a href="${legacy('/admin/tabelas')}"><i class="bi bi-table"></i> Tabelas</a></div></div>
  <div class="alv8-group"><button type="button"><i class="bi bi-broadcast-pin"></i> Operação <i class="bi bi-chevron-down"></i></button><div class="alv8-menu"><a href="/admin/leve/escala"><i class="bi bi-calendar-week"></i> Escala</a><a href="/admin/leve/trocas"><i class="bi bi-arrow-left-right"></i> Trocas</a><a href="/admin/leve/historico"><i class="bi bi-clock-history"></i> Histórico</a></div></div>
  <div class="alv8-group"><button type="button"><i class="bi bi-bar-chart"></i> Gestão <i class="bi bi-chevron-down"></i></button><div class="alv8-menu"><a href="${legacy('/admin/avaliacoes')}"><i class="bi bi-stars"></i> Avaliações</a><a href="${legacy('/admin/avisos')}"><i class="bi bi-megaphone"></i> Avisos</a></div></div>
  <a class="alv8-direct" href="${legacy('/admin?tab=config')}"><i class="bi bi-gear"></i> Configurações</a><span class="alv8-spacer"></span><a class="alv8-direct" href="/logout"><i class="bi bi-box-arrow-right"></i> Sair</a>
 </nav>`;document.body.prepend(header);
 header.querySelectorAll('.alv8-group>button').forEach(btn=>btn.addEventListener('click',e=>{e.stopPropagation();const g=btn.parentElement;header.querySelectorAll('.alv8-group').forEach(x=>{if(x!==g)x.classList.remove('open')});g.classList.toggle('open')}));document.addEventListener('click',()=>header.querySelectorAll('.alv8-group').forEach(x=>x.classList.remove('open')));
}
function destination(a){let u;try{u=new URL(a.href,location.origin)}catch(e){return ''}if(isLegacyUrl(u))return '';const tab=(a.dataset&&a.dataset.adminV6Tab||'').trim();if(TAB_MAP[tab])return TAB_MAP[tab];if(PATH_MAP[u.pathname])return PATH_MAP[u.pathname]+u.search;if(u.pathname==='/admin'){const t=(u.searchParams.get('tab')||'').trim();if(TAB_MAP[t])return TAB_MAP[t]}return ''}
document.addEventListener('click',function(e){const a=e.target.closest('a');if(!a)return;const d=destination(a);if(!d)return;e.preventDefault();e.stopImmediatePropagation();location.href=d;},true);
document.addEventListener('DOMContentLoaded',function(){cleanOld();topbar();setTimeout(()=>{cleanOld();topbar()},60);setTimeout(()=>{cleanOld();topbar()},350)});
})();