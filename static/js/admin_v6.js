(function(){
  'use strict';

  function tabNameFromLink(link){
    const target=(link.getAttribute('data-bs-target')||link.getAttribute('href')||'').trim();
    return target.startsWith('#') ? target.slice(1) : '';
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
      if(src.classList.contains('active')) a.classList.add('active');
      a.addEventListener('click',function(e){
        e.preventDefault();
        const pane=document.getElementById(tab);
        if(!pane) return;
        document.querySelectorAll('.main .tab-pane').forEach(p=>p.classList.remove('show','active'));
        pane.classList.add('show','active');
        document.querySelectorAll('.admin-v6-menu a[data-admin-v6-tab]').forEach(x=>x.classList.toggle('active',x.dataset.adminV6Tab===tab));
        const url=new URL(window.location.href);
        url.searchParams.set('tab',tab);
        url.searchParams.delete('ajax_partial');
        url.searchParams.delete('ajax');
        history.replaceState({},'',url.toString());
        document.querySelectorAll('.admin-v6-group').forEach(g=>g.classList.remove('open'));
        if(tab==='lancamentos') disableBrokenLancamentoAjax();
        window.scrollTo({top:0,behavior:'instant'});
      });
    }else{
      a.href=src.href||'#';
    }
    return a;
  }

  function makeGroup(title,links,iconClass){
    if(!links.length) return null;
    const group=document.createElement('div');
    group.className='admin-v6-group';
    const btn=document.createElement('button');
    btn.type='button';
    btn.className='admin-v6-group-btn';
    btn.innerHTML=`<i class="${iconClass||'bi bi-grid'}"></i><span>${title}</span><i class="bi bi-chevron-down admin-v6-caret"></i>`;
    const menu=document.createElement('div');
    menu.className='admin-v6-menu';
    links.forEach(src=>menu.appendChild(makeMenuLink(src)));
    btn.addEventListener('click',function(e){
      e.stopPropagation();
      document.querySelectorAll('.admin-v6-group').forEach(g=>{if(g!==group)g.classList.remove('open')});
      group.classList.toggle('open');
    });
    group.append(btn,menu);
    return group;
  }

  function buildHorizontalMenu(){
    if(document.getElementById('adminV6Topbar')) return;
    const sidebar=document.querySelector('.sidebar');
    const main=document.querySelector('.main');
    if(!sidebar||!main) return;

    const top=document.createElement('header');
    top.id='adminV6Topbar';
    top.className='admin-v6-topbar';
    top.innerHTML=`
      <div class="admin-v6-brand">
        <div class="admin-v6-brand-icon"><i class="bi bi-building-gear"></i></div>
        <div class="admin-v6-brand-copy"><small>PAINEL COOPEX</small><strong>Administração Central</strong></div>
      </div>
      <nav class="admin-v6-nav" id="adminV6Nav"></nav>`;
    document.body.insertBefore(top,document.body.firstChild);
    const nav=top.querySelector('#adminV6Nav');

    const allDirect=[...sidebar.querySelectorAll(':scope .sidebar-nav > .nav-link-main')];
    const footerDirect=[...sidebar.querySelectorAll('.sidebar-footer .nav-link-main')];
    const logout=footerDirect.find(a=>(a.getAttribute('href')||'').includes('logout'));

    const overview=[];
    allDirect.forEach(a=>{ if(!footerDirect.includes(a)) overview.push(a); });
    const overviewGroup=makeGroup('Visão Geral',overview,'bi bi-house-door');
    if(overviewGroup) nav.appendChild(overviewGroup);

    sidebar.querySelectorAll('.nav-link-group-toggle').forEach(toggle=>{
      const linksBox=toggle.nextElementSibling;
      if(!linksBox||!linksBox.classList.contains('nav-sublinks')) return;
      const title=(toggle.querySelector('.nav-link-label')?.textContent||'Menu').trim();
      const icon=toggle.querySelector('.nav-link-icon i')?.className||'bi bi-grid';
      const links=[...linksBox.querySelectorAll('a')];
      const group=makeGroup(title,links,icon);
      if(group) nav.appendChild(group);
    });

    const adminLinks=footerDirect.filter(a=>a!==logout);
    const adminGroup=makeGroup('Administração',adminLinks,'bi bi-gear');
    if(adminGroup) nav.appendChild(adminGroup);

    const spacer=document.createElement('span');
    spacer.className='admin-v6-spacer';
    nav.appendChild(spacer);

    if(logout){
      const out=document.createElement('a');
      out.className='admin-v6-direct admin-v6-logout';
      out.href=logout.href;
      out.innerHTML='<i class="bi bi-box-arrow-right"></i><span>Sair</span>';
      nav.appendChild(out);
    }

    document.addEventListener('click',()=>document.querySelectorAll('.admin-v6-group').forEach(g=>g.classList.remove('open')));
  }

  function ensureTabHidden(form){
    let input=form.querySelector('input[name="tab"]');
    if(!input){
      input=document.createElement('input');
      input.type='hidden';
      input.name='tab';
      form.appendChild(input);
    }
    input.value='lancamentos';
    form.querySelectorAll('input[name="ajax_partial"],input[name="ajax"]').forEach(x=>x.remove());
  }

  function disableBrokenLancamentoAjax(){
    const form=document.getElementById('formFiltroLancamentos');
    if(!form||form.dataset.adminV6Safe==='1') return;
    const clone=form.cloneNode(true);
    clone.dataset.adminV6Safe='1';
    clone.method='GET';
    ensureTabHidden(clone);
    clone.addEventListener('submit',function(){
      ensureTabHidden(clone);
      clone.querySelectorAll('[disabled]').forEach(el=>el.removeAttribute('disabled'));
    });
    form.replaceWith(clone);
  }

  function forceActionVisibility(){
    const pane=document.getElementById('lancamentos');
    if(!pane) return;
    pane.querySelectorAll('tbody tr').forEach(row=>{
      const cells=row.querySelectorAll('td');
      if(!cells.length) return;
      const action=cells[cells.length-1];
      action?.classList.add('admin-v6-action-cell');
      action?.querySelectorAll('button,a,form').forEach(el=>{
        el.style.removeProperty('display');
        el.style.visibility='visible';
        el.style.opacity='1';
      });
    });
  }

  function observeLancamentos(){
    const pane=document.getElementById('lancamentos');
    if(!pane) return;
    const obs=new MutationObserver(()=>{
      disableBrokenLancamentoAjax();
      forceActionVisibility();
    });
    obs.observe(pane,{childList:true,subtree:true});
  }

  document.addEventListener('DOMContentLoaded',function(){
    buildHorizontalMenu();
    // O listener AJAX antigo já foi registrado antes deste script. Substituir
    // o formulário remove esse listener sem alterar os outros filtros do Admin.
    disableBrokenLancamentoAjax();
    forceActionVisibility();
    observeLancamentos();
  });
})();
