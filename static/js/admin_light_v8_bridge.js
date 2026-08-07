(function(){'use strict';
const TAB_MAP={resumo:'/admin/leve/resumo',escalas:'/admin/leve/escala',cooperados:'/admin/leve/cooperados'};
const PATH_MAP={'/admin/avaliacoes':'/admin/leve/avaliacoes','/admin/documentos':'/admin/leve/documentos','/admin/tabelas':'/admin/leve/tabelas','/admin/avisos':'/admin/leve/avisos'};
function clean(){
  document.querySelectorAll('[data-admin-v6-tab="config"],a[href="#config"],a[href*="tab=config"]').forEach(a=>a.remove());
  document.querySelectorAll('.admin-v6-menu a').forEach(a=>{const t=(a.textContent||'').trim().toLowerCase();if(t==='sistemas'||t==='sistema')a.remove()});
  document.querySelectorAll('.admin-v6-group').forEach(g=>{const menu=g.querySelector('.admin-v6-menu');if(menu&&!menu.querySelector('a'))g.remove()});
}
function destination(a){
  const tab=(a.dataset&&a.dataset.adminV6Tab||'').trim();if(TAB_MAP[tab])return TAB_MAP[tab];
  let u;try{u=new URL(a.href,location.origin)}catch(e){return ''}
  if(PATH_MAP[u.pathname])return PATH_MAP[u.pathname]+u.search;
  if(u.pathname==='/admin'){
    const t=(u.searchParams.get('tab')||'').trim();if(TAB_MAP[t])return TAB_MAP[t];
  }
  return '';
}
document.addEventListener('click',function(e){const a=e.target.closest('a');if(!a)return;const d=destination(a);if(!d)return;e.preventDefault();e.stopImmediatePropagation();location.href=d;},true);
document.addEventListener('DOMContentLoaded',function(){clean();setTimeout(clean,50);setTimeout(clean,350)});
})();
