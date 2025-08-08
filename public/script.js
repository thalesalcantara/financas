// Inicialização Firebase
const firebaseConfig = {
  apiKey: "AIzaSyD32JNIb-IplbuAM8WD-Nw_P3gA5wNSNwY",
  authDomain: "kratosfinancas.firebaseapp.com",
  projectId: "kratosfinancas",
  storageBucket: "kratosfinancas.firebasestorage.app",
  messagingSenderId: "740158664268",
  appId: "1:740158664268:web:9e4583e75c94cbd90c0ee9"
};
firebase.initializeApp(firebaseConfig);
const db = firebase.firestore();
const auth = firebase.auth();

let appData = null;
let userEmail = null;

const mainPainel = document.getElementById('mainPainel');
const fabBtn = document.getElementById('fabBtn');
const modalContainer = document.getElementById('modal'); // Ajuste: modalContainer = modal

// Menu fixo, para renderizar dinâmico
const MENUS = [
  { id: "dashboard", label: "Dashboard", icon: "📊" },
  { id: "transactions", label: "Lançamentos", icon: "💸" },
  { id: "metas", label: "Metas", icon: "🎯" },
  { id: "contas", label: "Contas", icon: "🏦" },
  { id: "categorias", label: "Categorias", icon: "🏷️" },
  { id: "relatorios", label: "Relatórios", icon: "📈" },
  { id: "configuracoes", label: "Configurações", icon: "⚙️" }
];

// Renderiza menu no DOM
function renderMenu() {
  const menuList = document.getElementById('menuList');
  menuList.innerHTML = '';
  MENUS.forEach(menu => {
    const li = document.createElement('li');
    li.textContent = `${menu.icon} ${menu.label}`;
    li.dataset.panel = menu.id;
    li.style.userSelect = "none";
    li.onclick = () => {
      document.querySelectorAll('#menuList li').forEach(i => i.classList.remove('active'));
      li.classList.add('active');
      loadPanel(menu.id);
    };
    menuList.appendChild(li);
  });
}

// Carrega o painel específico
function loadPanel(panelId) {
  fabBtn.style.display = 'none'; // por padrão oculta fab
  mainPainel.innerHTML = '';

  if (panelId === 'dashboard') {
    fetch('dashboard.html').then(res => res.text()).then(html => {
      mainPainel.innerHTML = html;
      renderDashboard();
      fabBtn.style.display = 'none'; // Não mostra fab aqui (exemplo)
    });
  } 
  else if (panelId === 'transactions') {
    fetch('transactions.html').then(res => res.text()).then(html => {
      mainPainel.innerHTML = html;
      renderTransactions();
      setupFabForTransactions();
    });
  }
  else {
    mainPainel.innerHTML = `<section><h2>${panelId}</h2><p>Conteúdo não implementado ainda.</p></section>`;
  }
}

// Função exemplo para dashboard (implemente conforme sua necessidade)
function renderDashboard() {
  const dash = document.getElementById('dashboard');
  if (!dash) return;
  dash.innerHTML = `<h2>Dashboard</h2><p>Bem-vindo, ${userEmail}!</p>`;
}

// Função exemplo para transações (implemente conforme sua necessidade)
function renderTransactions() {
  const trans = document.getElementById('transactions');
  if (!trans) return;
  trans.innerHTML = `<h2>Transações</h2><p>Aqui vai a tabela e filtros.</p>`;
  // TODO: preencher tabela, filtros, eventos
}

// Configura botão flutuante no painel transações
function setupFabForTransactions() {
  fabBtn.style.display = 'flex';
  fabBtn.onclick = () => openTransactionModal();
}

// Abre modal de lançamento novo ou edição
function openTransactionModal(transactionIndex = null) {
  fetch('modal.html')
    .then(r => r.text())
    .then(html => {
      modalContainer.innerHTML = html;
      modalContainer.classList.add('active');
      modalContainer.style.display = 'flex';

      // Preencher selects com contas e categorias
      const txAccount = document.getElementById('txAccount');
      const txAccountTo = document.getElementById('txAccountTo');
      const txCategory = document.getElementById('txCategory');
      const accountToContainer = document.getElementById('accountToContainer');
      const txType = document.getElementById('txType');

      if (txAccount) txAccount.innerHTML = appData.contas.map(c => `<option>${c.nome}</option>`).join('');
      if (txAccountTo) txAccountTo.innerHTML = appData.contas.map(c => `<option>${c.nome}</option>`).join('');
      if (txCategory) txCategory.innerHTML = appData.categorias.map(c => `<option>${c.nome}</option>`).join('');

      txType.onchange = () => {
        if (!accountToContainer) return;
        accountToContainer.style.display = txType.value === 'transfer' ? 'block' : 'none';
      };
      txType.onchange();

      // Se for edição preenche os dados
      if (transactionIndex !== null && appData.transacoes && appData.transacoes[transactionIndex]) {
        const tx = appData.transacoes[transactionIndex];
        document.getElementById('modalTitle').textContent = 'Editar Lançamento';
        txType.value = tx.type;
        txAccount.value = tx.account;
        txAccountTo.value = tx.accountTo || '';
        txCategory.value = tx.category;
        document.getElementById('txDescription').value = tx.description || '';
        document.getElementById('txDate').value = tx.date || new Date().toISOString().slice(0, 10);
        document.getElementById('txValue').value = tx.value;
        document.getElementById('txNotes').value = tx.notes || '';
        txType.onchange();
      } else {
        document.getElementById('modalTitle').textContent = 'Novo Lançamento';
      }

      // Form submit
      const form = document.getElementById('transactionForm');
      form.onsubmit = e => {
        e.preventDefault();
        saveTransaction(transactionIndex);
      };

      // Cancelar modal
      document.getElementById('btnCancel').onclick = closeModal;
    });
}

// Fecha modal
function closeModal() {
  modalContainer.classList.remove('active');
  modalContainer.style.display = 'none';
  modalContainer.innerHTML = '';
}

// Salvar transação (novo ou editar)
function saveTransaction(transactionIndex) {
  const type = document.getElementById('txType').value;
  const account = document.getElementById('txAccount').value;
  const accountTo = document.getElementById('txAccountTo').value;
  const category = document.getElementById('txCategory').value;
  const description = document.getElementById('txDescription').value.trim();
  const date = document.getElementById('txDate').value;
  const value = parseFloat(document.getElementById('txValue').value);
  const notes = document.getElementById('txNotes').value.trim();

  if (!account || (type === 'transfer' && !accountTo) || !category || !date || !value) {
    alert('Por favor preencha todos os campos obrigatórios.');
    return;
  }

  // Atualiza saldo contas
  const contaOrigem = appData.contas.find(c => c.nome === account);
  if (!contaOrigem) {
    alert('Conta de origem inválida');
    return;
  }
  let contaDestino = null;
  if (type === 'transfer') {
    contaDestino = appData.contas.find(c => c.nome === accountTo);
    if (!contaDestino) {
      alert('Conta destino inválida');
      return;
    }
  }

  // Reverter saldo transação anterior (se edição)
  if (transactionIndex !== null) {
    const oldTx = appData.transacoes[transactionIndex];
    if (oldTx.type === 'income') {
      const c = appData.contas.find(c => c.nome === oldTx.account);
      if (c) c.saldo -= oldTx.value;
    } else if (oldTx.type === 'expense') {
      const c = appData.contas.find(c => c.nome === oldTx.account);
      if (c) c.saldo += oldTx.value;
    } else if (oldTx.type === 'transfer') {
      const cFrom = appData.contas.find(c => c.nome === oldTx.account);
      const cTo = appData.contas.find(c => c.nome === oldTx.accountTo);
      if (cFrom && cTo) {
        cFrom.saldo += oldTx.value;
        cTo.saldo -= oldTx.value;
      }
    }
  }

  // Aplica saldo com novos valores
  if (type === 'income') {
    contaOrigem.saldo += value;
  } else if (type === 'expense') {
    contaOrigem.saldo -= value;
  } else if (type === 'transfer') {
    contaOrigem.saldo -= value;
    contaDestino.saldo += value;
  }

  const transactionObj = {
    type,
    account,
    accountTo: type === 'transfer' ? accountTo : undefined,
    category,
    description,
    date,
    value,
    notes,
  };

  if (transactionIndex !== null) {
    appData.transacoes[transactionIndex] = transactionObj;
  } else {
    if (!appData.transacoes) appData.transacoes = [];
    appData.transacoes.push(transactionObj);
  }

  saveData();
  closeModal();
  loadPanel('transactions');
}

// Salva dados no Firestore
function saveData() {
  db.collection("usuarios").doc(userEmail).set(appData);
}

// Carrega dados do usuário
function loadUserData() {
  db.collection("usuarios").doc(userEmail).get().then(doc => {
    if (doc.exists) {
      appData = doc.data();
    } else {
      appData = getModeloZerado();
      saveData();
    }
    renderMenu();
    // Carrega painel inicial
    const firstMenuId = MENUS[0].id;
    // Marca primeiro menu ativo visualmente
    document.querySelectorAll('#menuList li').forEach(li => {
      li.classList.toggle('active', li.dataset.panel === firstMenuId);
    });
    loadPanel(firstMenuId);
    fabBtn.style.display = 'flex';
  });
}

// Modelo inicial caso não exista dados
function getModeloZerado() {
  return {
    user: userEmail,
    contas: [{ nome: "Carteira", saldo: 0, cor: "#246bfd" }],
    categorias: [
      { nome: "Salário", tipo: "income", cor: "#24c176" },
      { nome: "Alimentação", tipo: "expense", cor: "#e14c54" }
    ],
    transacoes: [],
    metas: [],
    config: { theme: "dark", alertas: [], limites: {}, excelSep: ";" },
    anexos: {}
  };
}

// Autenticação Firebase - controla redirecionamento se não logado
auth.onAuthStateChanged(user => {
  if (!user) {
    window.location.href = "login.html";
    return;
  }
  userEmail = user.email;
  loadUserData();
});

// Botão sair logout
document.getElementById('logoutBtn').onclick = () => {
  auth.signOut().then(() => {
    window.location.href = 'login.html';
  });
};

// Fecha modal ao clicar fora
modalContainer.onclick = (e) => {
  if (e.target === modalContainer) closeModal();
};
