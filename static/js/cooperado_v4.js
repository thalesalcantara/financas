document.addEventListener('DOMContentLoaded', function () {
  const MONEY_SOUND = '/static/dinheiro.mp3';
  let blockedMoneySound = false;

  function playMoneySound() {
    try {
      const audio = new Audio(MONEY_SOUND);
      audio.volume = 1;
      const promise = audio.play();
      if (promise && typeof promise.catch === 'function') {
        promise.catch(function () { blockedMoneySound = true; });
      }
    } catch (e) {
      blockedMoneySound = true;
    }
  }

  document.addEventListener('pointerdown', function () {
    if (!blockedMoneySound) return;
    blockedMoneySound = false;
    playMoneySound();
  }, { passive: true });

  // Confirmação sonora imediata ao cooperado quando ele envia quantidade/valor.
  document.querySelectorAll('form[data-coopex-production-submit="1"]').forEach(function (form) {
    form.addEventListener('submit', function (event) {
      if (form.dataset.coopexSoundSubmitted === '1') return;
      if (!form.checkValidity()) return;
      event.preventDefault();
      form.dataset.coopexSoundSubmitted = '1';
      playMoneySound();
      window.setTimeout(function () {
        HTMLFormElement.prototype.submit.call(form);
      }, 260);
    });
  });

  // Nova avaliação recebida: toca somente uma vez por avaliação.
  async function checkLatestEvaluation() {
    try {
      const response = await fetch('/api/coop/avaliacoes/latest', { cache: 'no-store' });
      if (!response.ok) return;
      const data = await response.json();
      if (!data || !data.ok || !data.cooperado_id) return;

      const latestId = Number(data.latest_id || 0);
      const storageKey = 'coopex:lastIncomingEvaluation:' + String(data.cooperado_id);
      const storedRaw = localStorage.getItem(storageKey);

      // Primeiro acesso neste navegador: cria a linha de base sem tocar avaliações antigas.
      if (storedRaw === null) {
        localStorage.setItem(storageKey, String(latestId));
        return;
      }

      const storedId = Number(storedRaw || 0);
      if (latestId > storedId) {
        // Grava antes de tocar para impedir repetição após refresh/reabertura.
        localStorage.setItem(storageKey, String(latestId));
        playMoneySound();
      }
    } catch (e) {}
  }

  checkLatestEvaluation();
  window.setInterval(checkLatestEvaluation, 20000);
});
