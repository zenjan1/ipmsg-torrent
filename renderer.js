const { ipcRenderer } = require('electron');
const P2PChat = require('./p2p');
const fs = require('fs');
const path = require('path');

let p2p;
let currentPeerId = null;
let chatHistory = new Map();
let pendingFile = null;

document.addEventListener('DOMContentLoaded', () => {
  initApp();
});

function initApp() {
  const usernameInput = document.getElementById('username');
  const defaultUsername = `用户_${Math.floor(Math.random() * 10000)}`;
  usernameInput.value = defaultUsername;

  p2p = new P2PChat();
  
  setupEventListeners();
  
  p2p.init(defaultUsername);
  
  usernameInput.addEventListener('change', () => {
    if (p2p.localInfo) {
      p2p.localInfo.username = usernameInput.value;
    }
  });
}

function setupEventListeners() {
  p2p.on('ready', (info) => {
    document.getElementById('my-id').textContent = `ID: ${info.id.substring(0, 8)}...`;
  });

  p2p.on('status', (status) => {
    const statusEl = document.getElementById('connection-status');
    statusEl.textContent = status === 'online' ? '在线' : '离线';
    statusEl.classList.toggle('online', status === 'online');
  });

  p2p.on('peers-updated', (peers) => {
    updatePeerList(peers);
    document.getElementById('peer-count').textContent = `用户数: ${peers.length}`;
  });

  p2p.on('peer-joined', (peer) => {
    console.log('Peer joined:', peer);
  });

  p2p.on('message', (message) => {
    handleReceivedMessage(message);
  });

  p2p.on('message-sent', (message) => {
    addMessageToChat(message, true);
  });

  p2p.on('file-offer', (offer) => {
    handleFileOffer(offer);
  });

  p2p.on('download-progress', (progress) => {
    console.log('Download progress:', progress);
  });

  document.getElementById('send-btn').addEventListener('click', sendMessage);
  document.getElementById('message-input').addEventListener('keypress', (e) => {
    if (e.key === 'Enter') sendMessage();
  });
  document.getElementById('attach-file').addEventListener('click', selectFile);
}

function updatePeerList(peers) {
  const container = document.getElementById('peers');
  container.innerHTML = '';

  peers.forEach(peer => {
    const peerEl = document.createElement('div');
    peerEl.className = `peer-item ${currentPeerId === peer.id ? 'active' : ''}`;
    peerEl.innerHTML = `
      <div class="peer-name">${peer.username}</div>
      <div class="peer-id">${peer.id.substring(0, 8)}...${peer.hostname}</div>
    `;
    peerEl.addEventListener('click', () => selectPeer(peer));
    container.appendChild(peerEl);
  });
}

function selectPeer(peer) {
  currentPeerId = peer.id;
  updatePeerList(Array.from(p2p.peers.values()));
  document.getElementById('chat-title').textContent = peer.username;
  document.getElementById('message-input').disabled = false;
  document.getElementById('send-btn').disabled = false;
  renderChatHistory(peer.id);
}

function sendMessage() {
  const input = document.getElementById('message-input');
  const content = input.value.trim();
  
  if (!content || !currentPeerId) return;

  if (pendingFile) {
    sendFile();
    return;
  }

  p2p.sendMessage(currentPeerId, content, 'text');
  input.value = '';
}

function handleReceivedMessage(message) {
  addMessageToChat(message, false);
  
  if (message.from === currentPeerId) {
    renderChatHistory(currentPeerId);
  }
}

function addMessageToChat(message, isSent) {
  const peerId = isSent ? message.to : message.from;
  
  if (!chatHistory.has(peerId)) {
    chatHistory.set(peerId, []);
  }
  
  chatHistory.get(peerId).push({
    ...message,
    isSent
  });
}

function renderChatHistory(peerId) {
  const container = document.getElementById('messages');
  container.innerHTML = '';
  
  const history = chatHistory.get(peerId) || [];
  
  history.forEach(msg => {
    const msgEl = createMessageElement(msg);
    container.appendChild(msgEl);
  });
  
  container.scrollTop = container.scrollHeight;
}

function createMessageElement(message) {
  const div = document.createElement('div');
  div.className = `message ${message.isSent ? 'sent' : 'received'}`;
  
  const time = new Date(message.timestamp).toLocaleTimeString('zh-CN', { 
    hour: '2-digit', 
    minute: '2-digit' 
  });

  if (message.contentType === 'file') {
    const fileId = 'file-' + message.id;
    div.innerHTML = `
      <div class="message-bubble">
        <div class="file-message" id="${fileId}">
          <span class="file-icon">📁</span>
          <div class="file-info">
            <div class="file-name">${message.content.name}</div>
            <div class="file-size">${formatFileSize(message.content.size)}</div>
          </div>
          ${!message.isSent ? '<button class="btn btn-primary" style="padding: 8px 16px;">下载</button>' : ''}
        </div>
      </div>
      <div class="message-time">${time}</div>
    `;
    
    if (!message.isSent) {
      setTimeout(() => {
        const downloadBtn = div.querySelector('button');
        if (downloadBtn) {
          downloadBtn.addEventListener('click', () => downloadFile(message));
        }
      }, 0);
    }
  } else {
    div.innerHTML = `
      <div class="message-bubble">${escapeHtml(message.content)}</div>
      <div class="message-time">${time}</div>
    `;
  }
  
  return div;
}

async function downloadFile(message) {
  try {
    const result = await ipcRenderer.invoke('select-directory');
    if (result.canceled) return;
    
    const savePath = result.filePaths[0];
    const magnetURI = message.content.magnetURI;
    
    alert('开始下载文件...\n保存位置: ' + savePath);
    
    await p2p.downloadFile(magnetURI, savePath);
    alert('文件下载完成！');
  } catch (error) {
    console.error('Download error:', error);
    alert('下载失败: ' + error.message);
  }
}

async function selectFile() {
  const result = await ipcRenderer.invoke('select-file');
  
  if (!result.canceled && result.filePaths.length > 0) {
    const filePath = result.filePaths[0];
    const stats = fs.statSync(filePath);
    
    pendingFile = {
      path: filePath,
      name: path.basename(filePath),
      size: stats.size
    };
    
    showFilePreview(pendingFile);
  }
}

function showFilePreview(file) {
  const preview = document.getElementById('file-preview');
  preview.innerHTML = `
    <div class="file-message">
      <span class="file-icon">📎</span>
      <div class="file-info">
        <div class="file-name">${file.name}</div>
        <div class="file-size">${formatFileSize(file.size)}</div>
      </div>
      <button class="btn btn-secondary" onclick="clearFile()" style="padding: 8px 12px;">✕</button>
    </div>
  `;
}

function clearFile() {
  pendingFile = null;
  document.getElementById('file-preview').innerHTML = '';
}

async function sendFile() {
  if (!pendingFile || !currentPeerId) return;

  try {
    const fileInfo = await p2p.shareFile(pendingFile.path);
    
    const messageContent = {
      name: pendingFile.name,
      size: pendingFile.size,
      magnetURI: fileInfo.magnetURI
    };
    
    p2p.sendMessage(currentPeerId, messageContent, 'file');
    clearFile();
    document.getElementById('message-input').value = '';
  } catch (error) {
    console.error('Error sending file:', error);
    alert('发送文件失败: ' + error.message);
  }
}

function handleFileOffer(offer) {
  const confirmAccept = confirm(`收到文件传输请求: ${offer.file.name}\n是否接受?`);
  p2p.respondToFileOffer(offer.id, confirmAccept, offer.from);
}

function formatFileSize(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

window.clearFile = clearFile;
