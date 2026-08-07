document.addEventListener('DOMContentLoaded',function(){
  const section=document.querySelector('section[data-view="lancar"]');
  if(!section)return;

  const buttons=section.querySelectorAll('[data-coopex-tab]');
  const approvalPanel=document.getElementById('coopexApprovalPanel');
  const mainRow=section.querySelector('.row.g-4.align-items-start');

  function selectTab(name){
    buttons.forEach(btn=>btn.classList.toggle('active',btn.dataset.coopexTab===name));
    const approvals=name==='approvals';
    if(approvalPanel)approvalPanel.classList.toggle('active',approvals);
    if(mainRow)mainRow.style.display=approvals?'none':'';
  }
  buttons.forEach(btn=>btn.addEventListener('click',()=>selectTab(btn.dataset.coopexTab)));

  const selectedPhoto=document.getElementById('selFoto');
  const expandedPhoto=document.getElementById('coopexExpandedPhoto');
  const photoModalElement=document.getElementById('coopexPhotoModal');
  if(selectedPhoto&&expandedPhoto&&photoModalElement){
    selectedPhoto.setAttribute('title','Clique para ampliar a foto');
    selectedPhoto.setAttribute('role','button');
    selectedPhoto.setAttribute('tabindex','0');
    const openPhoto=()=>{
      if(!selectedPhoto.src)return;
      expandedPhoto.src=selectedPhoto.src;
      expandedPhoto.alt=selectedPhoto.alt||'Foto ampliada do cooperado';
      bootstrap.Modal.getOrCreateInstance(photoModalElement).show();
    };
    selectedPhoto.addEventListener('click',openPhoto);
    selectedPhoto.addEventListener('keydown',event=>{
      if(event.key==='Enter'||event.key===' '){event.preventDefault();openPhoto();}
    });
  }

  const soundButton=document.getElementById('coopexSoundToggle');
  const pendingCount=Number(approvalPanel?.dataset.pendingCount||0);
  let currentPending=pendingCount;
  let soundOn=localStorage.getItem('coopexStrongApprovalSound')!=='0';
  let audioContext=null;
  let userInteracted=false;

  function refreshSoundButton(){
    if(!soundButton)return;
    soundButton.innerHTML=soundOn
      ?'<i class="bi bi-volume-up-fill"></i> Som forte ativo'
      :'<i class="bi bi-volume-mute-fill"></i> Ativar som';
  }
  function armSound(){
    userInteracted=true;
    if(!audioContext){
      const Ctx=window.AudioContext||window.webkitAudioContext;
      if(Ctx)audioContext=new Ctx();
    }
    if(audioContext?.state==='suspended')audioContext.resume().catch(()=>{});
  }
  function oscillatorAlarm(){
    armSound();
    if(!audioContext)return;
    [0,.22,.44].forEach((delay,index)=>{
      const osc=audioContext.createOscillator();
      const gain=audioContext.createGain();
      osc.type='square';
      osc.frequency.value=index%2?760:1080;
      gain.gain.setValueAtTime(.0001,audioContext.currentTime+delay);
      gain.gain.exponentialRampToValueAtTime(.65,audioContext.currentTime+delay+.02);
      gain.gain.exponentialRampToValueAtTime(.0001,audioContext.currentTime+delay+.18);
      osc.connect(gain);gain.connect(audioContext.destination);
      osc.start(audioContext.currentTime+delay);
      osc.stop(audioContext.currentTime+delay+.2);
    });
  }
  async function strongAlarm(){
    if(!soundOn||currentPending<=0)return;
    try{
      const audio=new Audio('/static/avisos.mp3');
      audio.volume=1;
      await audio.play();
      setTimeout(()=>{audio.currentTime=0;audio.play().catch(oscillatorAlarm)},650);
    }catch(e){oscillatorAlarm();}
  }

  document.addEventListener('pointerdown',()=>{
    const firstInteraction=!userInteracted;
    armSound();
    if(firstInteraction&&currentPending>0)strongAlarm();
  },{passive:true});

  soundButton?.addEventListener('click',()=>{
    soundOn=!soundOn;
    localStorage.setItem('coopexStrongApprovalSound',soundOn?'1':'0');
    armSound();refreshSoundButton();
    if(soundOn&&currentPending>0)strongAlarm();
  });
  refreshSoundButton();

  let lastLatest=Number(sessionStorage.getItem('coopexApprovalLatest')||0);
  let lastCount=Number(sessionStorage.getItem('coopexApprovalCount')||pendingCount);
  async function pollApprovals(){
    try{
      const response=await fetch('/api/rest/producoes/pendentes/status',{cache:'no-store'});
      if(!response.ok)return;
      const data=await response.json();
      const latest=Number(data.latest||0);
      const count=Number(data.count??data.pending_count??0);
      currentPending=count;

      if((lastLatest&&latest>lastLatest)||count>lastCount){
        strongAlarm();
        setTimeout(()=>location.reload(),900);
      }

      lastLatest=Math.max(lastLatest,latest);
      lastCount=count;
      sessionStorage.setItem('coopexApprovalLatest',String(lastLatest));
      sessionStorage.setItem('coopexApprovalCount',String(lastCount));
    }catch(e){}
  }

  // Atualiza a quantidade rapidamente, mas o lembrete sonoro persistente é a cada 5 minutos.
  setInterval(pollApprovals,15000);
  setInterval(()=>{
    if(currentPending>0)strongAlarm();
  },5*60*1000);
});
