(function(){
  'use strict';

  const FAST_PARTIALS=new Set(['resumo','receitas','despesas','coop_receitas','coop_despesas']);
  const loadedPartials=new Set();
  let loadingTab='';

  function tabNameFromLink(link){
    const target=(link.getAttribute('data-bs-target')||link.getAttribute('href')||'').trim();
    return target.startsWith('#') ? target.slice(1) : '';
  }

  function todayISO(){
    const d=new Date();
    const y=d.getFullYear();
    const m=String(d.getMonth()+1).padStart(2,'0');
    const day=String(d.getDate()).padStart(2,'0');
    return `${y}-${m}-${day}`;
  }

  function setActiveMenu(tab){
    document.querySelectorAll('.admin-v6-menu a[data-admin-v6-tab]').forEach(x=>x.classList.toggle('active',x.dataset.adminV6Tab===tab));
  }

  function activatePane(tab){
    const pane=document.getElementById(tab);
    if(!pane)return;
    document.querySelectorAll('.main .tab-pane').forEach(p=>p.classList.remove('show','active'));
    pane.classList.add('show','active');
    setActiveMenu(tab);
    const url=new URL(window.location.href);
    url.searchParams.set('tab',tab);
    url.searchParams.delete('ajax_partial');
    url.searchParams.delete('ajax');
    history.replaceState({},'',url.toString());
    document.querySelectorAll('.admin-v6-group').forEach(g=>g.classList.remove('open'));
    window.scrollTo({top:0,behavior:'instant'});
  }

  function executeScripts(root){
    if(!root)return;
    root.querySelectorAll('script').forEach(oldScript=>{
      const s=document.createElement('script');
      if(oldScript.src)s.src=oldScript.src;
      if(oldScript.type)s.type=oldScript.type;
      s.textContent=oldScript.textContent;
      oldScript.replaceWith(s);
    });
  }

  async function loadPartialTab(tab,force){
    if(!FAST_PARTIALS.has(tab))return false;
    if(loadingTab)return false;
    const current=document.getElementById(tab);
    if(!current)return false;
    if(loadedPartials.has(tab)&&!force){activatePane(tab);return true;}

    loadingTab=tab;
    const oldHTML=current.innerHTML;
    current.innerHTML='<div class="admin-v7-loading"><span class="spinner-border spinner-border-sm me-2"></span>Carregando dados…</div>';
    activatePane(tab);
    try{
      const url=new URL('/admin',window.location.origin);
      url.searchParams.set('tab',tab);
      url.searchParams.set('ajax_partial',tab);

      // Resumo padrão é somente o dia atual. Filtro manual substitui isso.
      const pageParams=new URLSearchParams(window.location.search||'');
      ['restaurante_id','cooperado_id','data_inicio','data_fim','considerar_periodo'].forEach(k=>{
        const v=pageParams.get(k);if(v)url.searchParams.set(k,v);
      });
      if(tab==='resumo'&&!url.searchParams.get('data_inicio')&&!url.searchParams.get('data_fim')){
        const today=todayISO();
        url.searchParams.set('data_inicio',today);
        url.searchParams.set('data_fim',today);
      }

      const r=await fetch(url.toString(),{credentials:'same-origin',cache:'no-store',headers:{'X-Requested-With':'XMLHttpRequest'}});
      if(!r.ok)throw new Error('Falha ao carregar');
      const html=await r.text();
      if(!html.trim())throw new Error('Resposta vazia');
      const holder=document.createElement('div');
      holder.innerHTML=html;
      let newPane=holder.querySelector(`#${CSS.escape(tab)}`);
      if(!newPane){
        newPane=document.createElement('section');
        newPane.id=tab;
        newPane.className='tab-pane fade show active';
        newPane.innerHTML=html;
      }
      current.replaceWith(newPane);
      executeScripts(newPane);
      loadedPartials.add(tab);
      activatePane(tab);
      bindPartialForms(tab);
      return true;
    }catch(e){
      const pane=document.getElementById(tab);
      if(pane)pane.innerHTML=oldHTML||'<div class="admin-v7-loading">Não foi possível carregar agora.</div>';
      return false;
    }finally{loadingTab='';}
  }

  function fullNavigateTab(tab){
    const u=new URL('/admin',window.location.origin);
    u.searchParams.set('tab',tab);
    window.location.href=u.toString();
  }

  function makeMenuLink(src){
    const a=document.createElement('a');
    const icon=src.querySelector('i')?.className||'bi bi-circle';
    const label=(src.querySelector('.nav-link-label')?.textContent||src.textContent||'').trim();
    const tab=tabNameFromLink(src);
    a.innerHTML=`<i class="${icon}"></i><span>${label}</span>`;

    if(tab){
      a.href=`#${tab}`;
      a.dataset.adminV6Tab=tab;
      if(src.classList.contains('active'))a.classList.add('active');
      a.addEventListener('click',async function(e){
        e.preventDefault();
        if(tab==='lancamentos'){
          const pane=document.getElementById('lancamentos');
          if(pane&&pane.querySelector('tbody tr')){activatePane(tab);disableBrokenLancamentoAjax();forceActionVisibility();}
          else fullNavigateTab(tab);
          return;
        }
        if(FAST_PARTIALS.has(tab)){
          const ok=await loadPartialTab(tab,false);
          if(!ok)fullNavigateTab(tab);
          return;
        }
        fullNavigateTab(tab);
      });
    }else{
      a.href=src.href||'#';
    }
    return a;
  }

  function makeGroup(title,links,iconClass){
    if(!links.length)return null;
    const group=document.createElement('div');group.className='admin-v6-group';
    const btn=document.createElement('button');btn.type='button';btn.className='admin-v6-group-btn';
    btn.innerHTML=`<i class="${iconClass||'bi bi-grid'}"></i><span>${title}</span><i class="bi bi-chevron-down admin-v6-caret"></i>`;
    const menu=document.createElement('div');menu.className='admin-v6-menu';
    links.forEach(src=>menu.appendChild(makeMenuLink(src)));
    btn.addEventListener('click',function(e){e.stopPropagation();document.querySelectorAll('.admin-v6-group').forEach(g=>{if(g!==group)g.classList.remove('open')});group.classList.toggle('open');});
    group.append(btn,menu);return group;
  }

  function buildHorizontalMenu(){
    if(document.getElementById('adminV6Topbar'))return;
    const sidebar=document.querySelector('.sidebar');
    const main=document.querySelector('.main');
    if(!sidebar||!main)return;
    const top=document.createElement('header');top.id='adminV6Topbar';top.className='admin-v6-topbar';
    top.innerHTML='<div class="admin-v6-brand"><div class="admin-v6-brand-icon"><i class="bi bi-building-gear"></i></div><div class="admin-v6-brand-copy"><small>PAINEL COOPEX</small><strong>Administração Central</strong></div></div><nav class="admin-v6-nav" id="adminV6Nav"></nav>';
    document.body.insertBefore(top,document.body.firstChild);
    const nav=top.querySelector('#adminV6Nav');
    const allDirect=[...sidebar.querySelectorAll(':scope .sidebar-nav > .nav-link-main')];
    const footerDirect=[...sidebar.querySelectorAll('.sidebar-footer .nav-link-main')];
    const logout=footerDirect.find(a=>(a.getAttribute('href')||'').includes('logout'));
    const overview=[];allDirect.forEach(a=>{if(!footerDirect.includes(a))overview.push(a)});
    const overviewGroup=makeGroup('Visão Geral',overview,'bi bi-house-door');if(overviewGroup)nav.appendChild(overviewGroup);
    sidebar.querySelectorAll('.nav-link-group-toggle').forEach(toggle=>{
      const linksBox=toggle.nextElementSibling;if(!linksBox||!linksBox.classList.contains('nav-sublinks'))return;
      const title=(toggle.querySelector('.nav-link-label')?.textContent||'Menu').trim();
      const icon=toggle.querySelector('.nav-link-icon i')?.className||'bi bi-grid';
      const group=makeGroup(title,[...linksBox.querySelectorAll('a')],icon);if(group)nav.appendChild(group);
    });
    const adminLinks=footerDirect.filter(a=>a!==logout);const adminGroup=makeGroup('Administração',adminLinks,'bi bi-gear');if(adminGroup)nav.appendChild(adminGroup);
    const spacer=document.createElement('span');spacer.className='admin-v6-spacer';nav.appendChild(spacer);
    if(logout){const out=document.createElement('a');out.className='admin-v6-direct admin-v6-logout';out.href=logout.href;out.innerHTML='<i class="bi bi-box-arrow-right"></i><span>Sair</span>';nav.appendChild(out)}
    document.addEventListener('click',()=>document.querySelectorAll('.admin-v6-group').forEach(g=>g.classList.remove('open')));
  }

  function ensureTabHidden(form){let input=form.querySelector('input[name="tab"]');if(!input){input=document.createElement('input');input.type='hidden';input.name='tab';form.appendChild(input)}input.value='lancamentos';form.querySelectorAll('input[name="ajax_partial"],input[name="ajax"]').forEach(x=>x.remove())}
  function disableBrokenLancamentoAjax(){const form=document.getElementById('formFiltroLancamentos');if(!form||form.dataset.adminV6Safe==='1')return;const clone=form.cloneNode(true);clone.dataset.adminV6Safe='1';clone.method='GET';ensureTabHidden(clone);clone.addEventListener('submit',function(){ensureTabHidden(clone);clone.querySelectorAll('[disabled]').forEach(el=>el.removeAttribute('disabled'))});form.replaceWith(clone)}
  function forceActionVisibility(){const pane=document.getElementById('lancamentos');if(!pane)return;pane.querySelectorAll('tbody tr').forEach(row=>{const cells=row.querySelectorAll('td');if(!cells.length)return;const action=cells[cells.length-1];action?.classList.add('admin-v6-action-cell');action?.querySelectorAll('button,a,form').forEach(el=>{el.style.removeProperty('display');el.style.visibility='visible';el.style.opacity='1'})})}
  function observeLancamentos(){const pane=document.getElementById('lancamentos');if(!pane)return;new MutationObserver(()=>{disableBrokenLancamentoAjax();forceActionVisibility()}).observe(pane,{childList:true,subtree:true})}

  function formParams(form){const fd=new FormData(form);const out=new URLSearchParams();fd.forEach((v,k)=>{if(v!==''&&v!==null)out.append(k,v)});return out}
  function bindPartialForms(tab){
    if(tab==='resumo'){
      const form=document.getElementById('formFiltroResumo');
      if(form&&!form.dataset.v7Bound){
        form.dataset.v7Bound='1';
        form.addEventListener('submit',async e=>{
          e.preventDefault();
          const params=formParams(form);const u=new URL(window.location.href);u.search='';u.searchParams.set('tab','resumo');
          params.forEach((v,k)=>u.searchParams.append(k,v));history.replaceState({},'',u.toString());loadedPartials.delete('resumo');await loadPartialTab('resumo',true);
        });
      }
      const month=document.getElementById('quickMonthResumo');
      const monthBtn=document.getElementById('btnQuickMonthResumo');
      if(monthBtn&&!monthBtn.dataset.v7Bound){monthBtn.dataset.v7Bound='1';monthBtn.addEventListener('click',async()=>{if(!month?.value)return;const [y,m]=month.value.split('-').map(Number);const start=`${y}-${String(m).padStart(2,'0')}-01`;const endDay=new Date(y,m,0).getDate();const end=`${y}-${String(m).padStart(2,'0')}-${String(endDay).padStart(2,'0')}`;const u=new URL(location.href);u.searchParams.set('tab','resumo');u.searchParams.set('data_inicio',start);u.searchParams.set('data_fim',end);history.replaceState({},'',u);loadedPartials.delete('resumo');await loadPartialTab('resumo',true)})}
      const year=document.getElementById('quickYearResumo');const yearBtn=document.getElementById('btnQuickYearResumo');
      if(yearBtn&&!yearBtn.dataset.v7Bound){yearBtn.dataset.v7Bound='1';yearBtn.addEventListener('click',async()=>{if(!year?.value)return;const y=year.value;const u=new URL(location.href);u.searchParams.set('tab','resumo');u.searchParams.set('data_inicio',`${y}-01-01`);u.searchParams.set('data_fim',`${y}-12-31`);history.replaceState({},'',u);loadedPartials.delete('resumo');await loadPartialTab('resumo',true)})}
    }
  }

  document.addEventListener('DOMContentLoaded',function(){
    buildHorizontalMenu();
    disableBrokenLancamentoAjax();
    forceActionVisibility();
    observeLancamentos();
    const params=new URLSearchParams(location.search);
    const tab=(params.get('tab')||'').trim();
    if(tab==='resumo')bindPartialForms('resumo');
  });
})();
