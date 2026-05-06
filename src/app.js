import { P2PChat } from './core/p2p.js';

class ChatApp {
  constructor() {
    this.p2p = null;
    this.currentPeerId = null;
    this.chatHistory = new Map();
    this.pendingFiles = [];
    this.currentFileOffer = null;
    this.typingTimeout = null;

    this.init();
  }

  async init() {
    const usernameInput = document.getElementById('username');
    const defaultUsername = `用户_${Math.floor(Math.random() * 10000)}`;
    usernameInput.value = defaultUsername;

    this.p2p = new P2PChat();

    this.setupEventListeners();
    this.setupMobileMenu();

    try {
      await this.p2p.init(defaultUsername);
      this.updatePlatformInfo();
    } catch (error) {
      console.error('Failed to initialize P2P:', error);
      this.showNotification('初始化失败', 'P2P连接初始化失败: ' + error.message);
    }

    usernameInput.addEventListener('change', () => {
      if (this.p2p && this.p2p.localInfo) {
        this.p2p.updateUsername(usernameInput.value);
      }
    });

    if (window.electronAPI) {
      window.electronAPI.onMenuOpenFile(() => {
        this.selectFile();
      });

      window.electronAPI.onMenuSaveHistory(() => {
        this.saveChatHistory();
      });
    }
  }

  setupEventListeners() {
    this.p2p.on('ready', (info) => {
      document.getElementById('my-id').textContent = info.id.substring(0, 8) + '...';
      console.log('P2P Ready:', info);
    });

    this.p2p.on('status', (status) => {
      const statusEl = document.getElementById('connection-status');
      statusEl.textContent = status === 'online' ? '在线' : '离线';
      statusEl.classList.toggle('online', status === 'online');
    });

    this.p2p.on('peers-updated', (peers) => {
      this.updatePeerList(peers);
    });

    this.p2p.on('peer-joined', (peer) => {
      console.log('Peer joined:', peer);
      this.showNotification('新用户加入', `${peer.username} 已上线`);
    });

    this.p2p.on('peer-left', (peer) => {
      console.log('Peer left:', peer);
      this.showNotification('用户离开', `${peer.username} 已离线`);

      if (this.currentPeerId === peer.id) {
        this.currentPeerId = null;
        this.clearChatArea();
      }
    });

    this.p2p.on('message', (message) => {
      this.handleReceivedMessage(message);
    });

    this.p2p.on('message-sent', (message) => {
      this.addMessageToChat(message, true);
    });

    this.p2p.on('file-offer', (offer) => {
      this.handleFileOffer(offer);
    });

    this.p2p.on('file-accept', (data) => {
      console.log('File accept received:', data);
    });

    this.p2p.on('file-reject', (data) => {
      console.log('File reject received:', data);
      this.showNotification('文件传输被拒绝', '对方拒绝了文件传输请求');
    });

    this.p2p.on('typing', (data) => {
      if (data.from === this.currentPeerId) {
        this.showTypingIndicator(data.fromName);
      }
    });

    document.getElementById('send-btn').addEventListener('click', () => this.sendMessage());
    document.getElementById('message-input').addEventListener('keypress', (e) => {
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        this.sendMessage();
      }
    });

    document.getElementById('message-input').addEventListener('input', () => {
      if (this.currentPeerId) {
        clearTimeout(this.typingTimeout);
        this.p2p.sendTyping(this.currentPeerId);
        this.typingTimeout = setTimeout(() => {
        }, 3000);
      }
    });

    document.getElementById('attach-file').addEventListener('click', () => this.selectFile());
    document.getElementById('file-input').addEventListener('change', (e) => this.handleFileSelect(e));

    document.getElementById('accept-file-btn').addEventListener('click', () => this.acceptFileOffer());
    document.getElementById('reject-file-btn').addEventListener('click', () => this.rejectFileOffer());

    document.getElementById('chat-sidebar-toggle').addEventListener('click', () => {
      document.getElementById('sidebar').classList.toggle('show');
    });
  }

  setupMobileMenu() {
    const mobileMenuBtn = document.getElementById('mobile-menu-btn');
    const sidebar = document.getElementById('sidebar');

    if (mobileMenuBtn) {
      mobileMenuBtn.addEventListener('click', () => {
        sidebar.classList.toggle('show');
      });
    }

    document.addEventListener('click', (e) => {
      if (window.innerWidth <= 768) {
        if (!sidebar.contains(e.target) && !mobileMenuBtn.contains(e.target)) {
          sidebar.classList.remove('show');
        }
      }
    });
  }

  updatePeerList(peers) {
    const container = document.getElementById('peers');
    const peerCount = document.getElementById('peer-count');
    const peerCountFooter = document.getElementById('peer-count-footer');

    peerCount.textContent = peers.length;
    peerCountFooter.textContent = peers.length;

    if (peers.length === 0) {
      container.innerHTML = `
        <div class="empty-peers">
          <div class="icon">👥</div>
          <p>暂无在线用户</p>
          <p style="font-size: 12px; margin-top: 8px;">与他人打开此页面即可连接</p>
        </div>
      `;
      return;
    }

    container.innerHTML = '';
    peers.forEach(peer => {
      const peerEl = document.createElement('div');
      peerEl.className = `peer-item ${this.currentPeerId === peer.id ? 'active' : ''}`;
      peerEl.innerHTML = `
        <div class="peer-name">${this.escapeHtml(peer.username)}</div>
        <div class="peer-hostname">${this.escapeHtml(peer.hostname || peer.platform || 'Unknown')}</div>
      `;
      peerEl.addEventListener('click', () => this.selectPeer(peer));
      container.appendChild(peerEl);
    });
  }

  selectPeer(peer) {
    this.currentPeerId = peer.id;

    const peers = this.p2p.getAllPeers();
    this.updatePeerList(peers);

    document.getElementById('chat-header').style.display = 'flex';
    document.getElementById('chat-title').textContent = peer.username;
    document.getElementById('chat-peer-info').textContent = `${peer.hostname || 'Unknown'} • ${peer.platform || 'Unknown'}`;
    document.getElementById('message-input').disabled = false;
    document.getElementById('send-btn').disabled = false;

    document.getElementById('no-chat').style.display = 'none';
    this.renderChatHistory(peer.id);

    if (window.innerWidth <= 768) {
      document.getElementById('sidebar').classList.remove('show');
    }
  }

  clearChatArea() {
    document.getElementById('chat-header').style.display = 'none';
    document.getElementById('chat-title').textContent = '选择用户开始聊天';
    document.getElementById('message-input').disabled = true;
    document.getElementById('send-btn').disabled = true;
    document.getElementById('no-chat').style.display = 'flex';

    const container = document.getElementById('messages');
    container.innerHTML = `
      <div class="no-chat-selected" id="no-chat">
        <div class="icon">💬</div>
        <h3>开始聊天</h3>
        <p>从左侧选择一个在线用户</p>
      </div>
    `;
  }

  sendMessage() {
    const input = document.getElementById('message-input');
    const content = input.value.trim();

    if (!content || !this.currentPeerId) return;

    if (this.pendingFiles.length > 0) {
      this.sendFiles();
      return;
    }

    this.p2p.sendMessage(this.currentPeerId, content, 'text');
    input.value = '';
  }

  handleReceivedMessage(message) {
    this.addMessageToChat(message, false);

    if (message.from === this.currentPeerId) {
      this.renderChatHistory(message.from);
    } else {
      const sender = this.p2p.getPeer(message.from);
      const senderName = sender ? sender.username : '未知用户';

      if (message.contentType === 'file') {
        this.showNotification('收到文件', `${senderName} 发送了文件: ${message.content.name}`);
      } else {
        this.showNotification('新消息', `${senderName}: ${message.content.substring(0, 50)}${message.content.length > 50 ? '...' : ''}`);
      }
    }
  }

  addMessageToChat(message, isSent) {
    const peerId = isSent ? message.to : message.from;

    if (!this.chatHistory.has(peerId)) {
      this.chatHistory.set(peerId, []);
    }

    this.chatHistory.get(peerId).push({
      ...message,
      isSent
    });
  }

  renderChatHistory(peerId) {
    const container = document.getElementById('messages');
    container.innerHTML = '';

    const history = this.chatHistory.get(peerId) || [];

    if (history.length === 0) {
      container.innerHTML = `
        <div style="flex: 1; display: flex; align-items: center; justify-content: center; color: #999;">
          开始发送消息吧
        </div>
      `;
      return;
    }

    history.forEach(msg => {
      const msgEl = this.createMessageElement(msg);
      container.appendChild(msgEl);
    });

    container.scrollTop = container.scrollHeight;
  }

  createMessageElement(message) {
    const div = document.createElement('div');
    div.className = `message ${message.isSent ? 'sent' : 'received'}`;

    const time = new Date(message.timestamp).toLocaleTimeString('zh-CN', {
      hour: '2-digit',
      minute: '2-digit'
    });

    if (message.contentType === 'file') {
      div.innerHTML = `
        <div class="message-bubble">
          <div class="file-message">
            <span class="file-icon">📁</span>
            <div class="file-info">
              <div class="file-name">${this.escapeHtml(message.content.name)}</div>
              <div class="file-size">${this.formatFileSize(message.content.size)}</div>
            </div>
          </div>
          ${!message.isSent && message.content.data ?
            `<div class="file-actions"><button class="btn btn-primary btn-small download-btn">保存</button></div>` :
            ''}
        </div>
        <div class="message-time">${time}</div>
      `;

      if (!message.isSent && message.content.data) {
        const downloadBtn = div.querySelector('.download-btn');
        if (downloadBtn) {
          downloadBtn.addEventListener('click', () => this.downloadFile(message));
        }
      }
    } else {
      div.innerHTML = `
        <div class="message-bubble">${this.escapeHtml(message.content)}</div>
        <div class="message-time">${time}</div>
      `;
    }

    return div;
  }

  async downloadFile(message) {
    try {
      const buffer = this.p2p.base64ToArrayBuffer(message.content.data);
      const blob = new Blob([buffer]);

      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = message.content.name;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);

      this.showNotification('下载完成', `文件 ${message.content.name} 已保存`);
    } catch (error) {
      console.error('Download error:', error);
      this.showNotification('下载失败', error.message);
    }
  }

  selectFile() {
    document.getElementById('file-input').click();
  }

  handleFileSelect(e) {
    const files = e.target.files;

    if (files.length === 0) return;

    this.pendingFiles = Array.from(files);
    this.showFilePreview();

    if (!this.currentPeerId) {
      this.showNotification('提示', '请先选择一个聊天用户');
    }
  }

  showFilePreview() {
    const container = document.getElementById('file-preview-container');
    const preview = document.getElementById('file-preview');

    if (this.pendingFiles.length === 0) {
      container.classList.remove('active');
      return;
    }

    container.classList.add('active');

    if (this.pendingFiles.length === 1) {
      const file = this.pendingFiles[0];
      preview.innerHTML = `
        <span class="file-icon">📎</span>
        <div class="file-info">
          <div class="file-name">${this.escapeHtml(file.name)}</div>
          <div class="file-size">${this.formatFileSize(file.size)}</div>
        </div>
        <button class="remove-btn" onclick="window.chatApp.clearFiles()">✕</button>
      `;
    } else {
      const totalSize = this.pendingFiles.reduce((sum, f) => sum + f.size, 0);
      preview.innerHTML = `
        <span class="file-icon">📎</span>
        <div class="file-info">
          <div class="file-name">${this.pendingFiles.length} 个文件</div>
          <div class="file-size">总计: ${this.formatFileSize(totalSize)}</div>
        </div>
        <button class="remove-btn" onclick="window.chatApp.clearFiles()">✕</button>
      `;
    }

    window.chatApp = this;
  }

  clearFiles() {
    this.pendingFiles = [];
    document.getElementById('file-preview-container').classList.remove('active');
    document.getElementById('file-input').value = '';
  }

  async sendFiles() {
    if (!this.currentPeerId || this.pendingFiles.length === 0) return;

    try {
      for (const file of this.pendingFiles) {
        const fileInfo = await this.p2p.shareFile(file, file.name, file.size);
        this.p2p.sendFileOffer(this.currentPeerId, fileInfo);

        const message = {
          type: 'chat',
          id: this.generateId(),
          from: this.p2p.getMyId(),
          to: this.currentPeerId,
          contentType: 'file',
          content: fileInfo,
          timestamp: Date.now()
        };
        this.addMessageToChat(message, true);
      }

      this.renderChatHistory(this.currentPeerId);
      this.clearFiles();

      this.showNotification('发送成功', `已发送 ${this.pendingFiles.length} 个文件`);
    } catch (error) {
      console.error('Error sending files:', error);
      this.showNotification('发送失败', error.message);
    }
  }

  handleFileOffer(offer) {
    this.currentFileOffer = offer;

    document.getElementById('offer-file-name').textContent = offer.file.name;
    document.getElementById('offer-file-size').textContent = this.formatFileSize(offer.file.size);
    document.getElementById('offer-file-from').textContent = offer.fromName || '未知用户';

    document.getElementById('file-offer-modal').classList.add('show');
  }

  async acceptFileOffer() {
    if (!this.currentFileOffer) return;

    const offer = this.currentFileOffer;
    document.getElementById('file-offer-modal').classList.remove('show');

    this.p2p.respondToFileOffer(offer.id, true, offer.from);

    try {
      const message = {
        type: 'chat',
        id: this.generateId(),
        from: offer.from,
        to: this.p2p.getMyId(),
        contentType: 'file',
        content: offer.file,
        timestamp: Date.now()
      };
      this.addMessageToChat(message, false);

      if (offer.from === this.currentPeerId) {
        this.renderChatHistory(offer.from);
      }

      this.showNotification('文件传输已接受', `正在接收文件: ${offer.file.name}`);
    } catch (error) {
      console.error('Error accepting file:', error);
      this.showNotification('接收失败', error.message);
    }

    this.currentFileOffer = null;
  }

  rejectFileOffer() {
    if (!this.currentFileOffer) return;

    this.p2p.respondToFileOffer(this.currentFileOffer.id, false, this.currentFileOffer.from);
    document.getElementById('file-offer-modal').classList.remove('show');
    this.currentFileOffer = null;
  }

  showTypingIndicator(senderName) {
    const indicator = document.getElementById('typing-indicator');
    indicator.textContent = `${senderName} 正在输入...`;
    indicator.style.display = 'block';

    clearTimeout(this.typingTimeout);
    this.typingTimeout = setTimeout(() => {
      indicator.style.display = 'none';
    }, 3000);
  }

  updatePlatformInfo() {
    const platformInfo = document.getElementById('platform-info');
    const connectionType = document.getElementById('connection-type');

    const ua = navigator.userAgent;
    let platform = 'Unknown';

    if (/mobile|android|iphone/i.test(ua)) {
      platform = 'Mobile Web';
    } else if (/tablet|ipad/i.test(ua)) {
      platform = 'Tablet Web';
    } else {
      platform = 'Desktop Web';
    }

    platformInfo.textContent = `平台: ${platform}`;

    if (typeof BroadcastChannel !== 'undefined') {
      connectionType.textContent = '连接: BroadcastChannel';
    } else {
      connectionType.textContent = '连接: 降级模式';
    }
  }

  showNotification(title, message) {
    const notification = document.getElementById('notification');
    const titleEl = document.getElementById('notification-title');
    const messageEl = document.getElementById('notification-text');

    titleEl.textContent = title;
    messageEl.textContent = message;
    notification.classList.add('show');

    setTimeout(() => {
      notification.classList.remove('show');
    }, 4000);

    if (window.electronAPI) {
      window.electronAPI.showNotification({ title, body: message });
    }
  }

  async saveChatHistory() {
    if (!this.chatHistory || this.chatHistory.size === 0) {
      this.showNotification('提示', '没有聊天记录可保存');
      return;
    }

    const historyData = {
      exportTime: new Date().toISOString(),
      myId: this.p2p.getMyId(),
      chats: []
    };

    for (const [peerId, messages] of this.chatHistory.entries()) {
      const peer = this.p2p.getPeer(peerId);
      historyData.chats.push({
        peerId,
        peerName: peer ? peer.username : 'Unknown',
        messages: messages
      });
    }

    const json = JSON.stringify(historyData, null, 2);

    if (window.electronAPI) {
      await window.electronAPI.saveFile({
        content: json,
        defaultPath: `chat-history-${Date.now()}.json`
      });
    } else {
      const blob = new Blob([json], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `chat-history-${Date.now()}.json`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    }

    this.showNotification('保存成功', '聊天记录已保存');
  }

  formatFileSize(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }

  escapeHtml(text) {
    if (typeof text !== 'string') return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
  }

  generateId() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
      const r = Math.random() * 16 | 0;
      const v = c === 'x' ? r : (r & 0x3 | 0x8);
      return v.toString(16);
    });
  }
}

document.addEventListener('DOMContentLoaded', () => {
  window.chatApp = new ChatApp();
});
