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

const menuItems = document.querySelectorAll('#menuList li');
const mainPainel = document.getElementById('mainPainel');
const fabBtn = document.getElementById('fabBtn');
const modalContainer = document.getElementById('modalContainer');

auth.onAuthStateChanged(user => {
  if (!user) {
    window.location.href = "login.html";
    return;
  }
  userEmail = user.email;
  loadUserData();
});

function loadUserData() {
  db.collection("usuarios").doc(userEmail).get().then(doc => {
    if (doc.exists) {
      appData = doc.data();
    } else {
      appData = getModeloZerado();
      saveData();
    }
    renderMenu();
    loadPanel('dashboard');
    fabBtn.style.display = 'flex';
  });
}

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

function saveData() {
  db.collection("usuarios").doc(userEmail).set(appData);
}

function renderMenu() {
  menuItems.forEach(li => {
    li.onclick = () => {
      menuItems.forEach(i => i.classList.remove('active'));
      li.classList.add('active');
      loadPanel(li.dataset.panel);
    };
  });
}

function loadPanel(panel) {
  mainPainel.innerHTML = ''; // limpa o conteúdo

  if (panel === 'dashboard') {
    fetch('dashboard.html')
      .then(r => r.text())
      .then(html => {
        mainPainel.innerHTML = html;
        renderDashboard(appData);
      });
  }
  else if (panel === 'transactions') {
    fetch('transactions.html')
      .then(r => r.text())
      .then(html => {
        mainPainel.innerHTML = html;
        renderTransactions(appData);
        setupFabForTransactions();
      });
  }
  // Repita para outros painéis: metas, contas, categorias, relatorios, configuracoes
  else {
    mainPainel.innerHTML = `<section><h2>${panel}</h2><p>Conteúdo não implementado ainda.</p></section>`;
    fabBtn.style.display = 'none';
  }
}

function renderDashboard(appData) {
  // Implementar a lógica do dashboard (ver dashboard.html script)
  // Exemplo: preencher saldo, receitas, despesas e gráfico
  // (Código de dashboard.html pode ser copiado aqui ou modularizado)
}

function renderTransactions(appData) {
  // Implemente o preenchimento da tabela de transações, filtros, eventos etc
  // Código detalhado para exibir os lançamentos, filtrar, editar e excluir
  // Exemplo: preencher contas e categorias nos selects, preencher tabela
}

function setupFabForTransactions() {
  fabBtn.onclick = () => {
    openTransactionModal();
  };
  fabBtn.style.display = 'flex';
}

function openTransactionModal(transactionIndex = null) {
  fetch('modal.html')
    .then(r => r.text())
    .then(html => {
      modalContainer.innerHTML = html;
      modalContainer.style.display = 'flex';

      // Preencher selects de contas e categorias
      const txAccount = document.getElementById('txAccount');
      const txAccountTo = document.getElementById('txAccountTo');
      const txCategory = document.getElementById('txCategory');
      const accountToContainer = document.getElementById('accountToContainer');
      const txType = document.getElementById('txType');

      txAccount.innerHTML = appData.contas.map(c => `<option>${c.nome}</option>`).join('');
      txAccountTo.innerHTML = appData.contas.map(c => `<option>${c.nome}</option>`).join('');
      txCategory.innerHTML = appData.categorias.map(c => `<option>${c.nome}</option>`).join('');

      txType.onchange = () => {
        accountToContainer.style.display = txType.value === 'transfer' ? 'block' : 'none';
      };
      txType.onchange();

      // Se for editar, preencher dados
      if (transactionIndex !== null) {
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
      }

      const form = document.getElementById('transactionForm');
      form.onsubmit = e => {
        e.preventDefault();
        // salvar dados no appData e firestore
        saveTransaction(transactionIndex);
      };

      document.getElementById('btnCancel').onclick = closeModal;
    });
}

function closeModal() {
  modalContainer.style.display = 'none';
  modalContainer.innerHTML = '';
}

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

  // Ajusta saldo das contas baseando-se no tipo da transação
  if (transactionIndex !== null) {
    // Reverter o saldo da transação anterior antes de atualizar
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
    appData.transacoes.push(transactionObj);
  }

  saveData();
  closeModal();
  loadPanel('transactions');
}

document.getElementById('logoutBtn').onclick = () => {
  auth.signOut().then(() => {
    window.location.href = 'login.html';
  });
};
