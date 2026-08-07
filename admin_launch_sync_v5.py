from __future__ import annotations

from flask import jsonify, session
from sqlalchemy import func

import approval_rejection_v5 as rejection

app = rejection.app
upgrade = rejection.upgrade
db = upgrade.db
Lancamento = upgrade.Lancamento

BUILD = "20260807-1104"


def _is_admin() -> bool:
    return (session.get("user_tipo") or "").strip().lower() == "admin"


if "admin_lancamentos_latest_v5" not in app.view_functions:
    @app.get("/api/admin/lancamentos/latest", endpoint="admin_lancamentos_latest_v5")
    def admin_lancamentos_latest_v5():
        if not _is_admin():
            return jsonify(ok=False), 403
        latest = db.session.query(func.max(Lancamento.id)).scalar() or 0
        return jsonify(ok=True, latest=int(latest))


def _install_admin_live_sync() -> None:
    loader = app.jinja_loader
    if not loader or getattr(loader, "_coopex_admin_launch_sync_v5", False):
        return

    original_get_source = loader.get_source
    live_sync_js = r'''
<script id="coopexAdminLaunchSyncV5">
(function(){
  let lastLatest=null;
  let checking=false;
  let reloading=false;

  function lancamentosAtivo(){
    const p=new URLSearchParams(window.location.search||'');
    if((p.get('tab')||'lancamentos')==='lancamentos') return true;
    const pane=document.getElementById('lancamentos');
    return !!pane && (pane.classList.contains('active') || pane.classList.contains('show'));
  }

  function safeReloadLancamentos(){
    if(reloading) return;
    reloading=true;
    const url=new URL(window.location.href);
    url.searchParams.set('tab','lancamentos');
    url.searchParams.delete('ajax_partial');
    url.searchParams.delete('ajax');
    url.searchParams.set('_sync',String(Date.now()));
    window.location.replace(url.toString());
  }

  async function checkLatest(){
    if(checking || reloading || document.visibilityState==='hidden') return;
    checking=true;
    try{
      const r=await fetch('/api/admin/lancamentos/latest',{credentials:'same-origin',cache:'no-store'});
      if(!r.ok) return;
      const data=await r.json();
      const latest=Number(data.latest||0);
      if(lastLatest===null){
        lastLatest=latest;
        return;
      }
      if(latest>lastLatest){
        lastLatest=latest;
        if(lancamentosAtivo()) safeReloadLancamentos();
      }
    }catch(e){}finally{checking=false;}
  }

  document.addEventListener('DOMContentLoaded',function(){
    // Apenas memoriza o último ID. Não substitui mais a tabela por parcial.
    setTimeout(checkLatest,350);
    setInterval(checkLatest,15000);
    window.addEventListener('focus',checkLatest);
    document.addEventListener('visibilitychange',()=>{
      if(document.visibilityState==='visible') checkLatest();
    });
  });
})();
</script>
'''

    def get_source(environment, template):
        source, filename, uptodate = original_get_source(environment, template)
        if template == "admin_dashboard.html":
            if "coopexAdminLaunchSyncV5" not in source:
                source = source.replace("</body>", live_sync_js + "\n</body>", 1)
        return source, filename, uptodate

    loader.get_source = get_source
    loader._coopex_admin_launch_sync_v5 = True
    app.jinja_env.cache.clear()


_install_admin_live_sync()
