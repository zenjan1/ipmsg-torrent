import { P2PChat } from './core/p2p.js';

class PPXApp {
  constructor() {
    this.p2p = null;
    this.currentTab = 'chats';
    this.currentChat = null;
    this.chatHistory = new Map();
    this.unreadCount = new Map();
    this.downloads = new Map();
    this.files = [];
    this.currentFileOffer = null;
    this.typingTimer = null;
    this.pendingFiles = [];
    
    this.init();
  }

  init() {
    const defaultName = localStorage.getItem('ppx_username') || `用户${Math.floor(Math.random() * 9999)}`;
    
    this.p2p = new P2PChat();
    this.setupTabs();
    this.setupChat();
    this.setupDownloads();
    this.setupProfile();
    this.setupModals();
    
    this.p2p.init(defaultName).then(info => {
      document.getElementById('my-name').textContent = info.username;
      document.getElementById('my-id').textContent = info.id.substring(0, 8);
      document.getElementById('my-avatar').textContent = info.username.substring(0, 1);
      this.updateConnectionInfo();
    }).catch(err => console.error('P2P init failed:', err));

    this.p2p.on('peers-updated', peers => this.renderPeers(peers));
    this.p2p.on('peer-joined', peer => this.showToast(`${peer.username} 上线`));
    this.p2p.on('peer-left', peer => {
      this.showToast(`${peer.username} 离线`);
      if (this.currentChat === peer.id) this.closeChat();
    });
    this.p2p.on('message', msg => this.handleMessage(msg));
    this.p2p.on('typing', data => this.showTyping(data));
    this.p2p.on('file-offer', offer => this.showFileOffer(offer));
  }

  setupTabs() {
    document.querySelectorAll('.tab-item').forEach(tab => {
      tab.addEventListener('click', () => {
        const tabName = tab.dataset.tab;
        this.switchTab(tabName);
      });
    });

    document.querySelectorAll('.file-tab').forEach(tab => {
      tab.addEventListener('click', () => {
        document.querySelectorAll('.file-tab').forEach(t => t.classList.remove('active'));
        tab.classList.add('active');
        this.renderFiles(tab.dataset.type);
      });
    });
  }

  switchTab(name) {
    this.currentTab = name;
    document.querySelectorAll('.tab-item').forEach(t => t.classList.remove('active'));
    document.querySelector(`.tab-item[data-tab="${name}"]`).classList.add('active');
    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
    document.getElementById(`${name}-tab`).classList.add('active');
    
    if (name === 'chats') {
      this.renderChatList();
      this.clearUnread(this.currentChat);
    } else if (name === 'files') {
      this.renderFiles('all');
    } else if (name === 'downloads') {
      this.renderDownloads();
    }
  }

  renderPeers(peers) {
    document.getElementById('online-count').textContent = peers.length;
    
    peers.forEach(peer => {
      if (!this.chatHistory.has(peer.id)) {
        this.chatHistory.set(peer.id, {
          id: peer.id,
          name: peer.username,
          avatar: peer.username.substring(0, 1),
          messages: [],
          lastTime: 0,
          lastMsg: ''
        });
      }
    });
    
    if (this.currentTab === 'chats') this.renderChatList();
  }

  renderChatList() {
    const container = document.getElementById('chats-list');
    const chats = Array.from(this.chatHistory.values())
      .filter(c => c.messages.length > 0)
      .sort((a, b) => b.lastTime - a.lastTime);

    if (chats.length === 0) {
      container.innerHTML = this.emptyState('💬', '暂无聊天记录');
      return;
    }

    container.innerHTML = chats.map(chat => {
      const unread = this.unreadCount.get(chat.id) || 0;
      return `
        <div class="list-item" data-id="${chat.id}">
          <div class="avatar">${chat.avatar}</div>
          <div class="chat-info">
            <div class="chat-name">${this.escape(chat.name)}</div>
            <div class="chat-preview">${this.getPreview(chat.lastMsg)}</div>
          </div>
          <div class="chat-meta">
            <div class="chat-time">${this.formatTime(chat.lastTime)}</div>
            ${unread > 0 ? `<div class="unread-count">${unread}</div>` : ''}
          </div>
        </div>
      `;
    }).join('');

    container.querySelectorAll('.list-item').forEach(item => {
      item.addEventListener('click', () => this.openChat(item.dataset.id));
    });
  }

  openChat(peerId) {
    const chat = this.chatHistory.get(peerId);
    if (!chat) return;
    
    this.currentChat = peerId;
    this.clearUnread(peerId);
    
    document.getElementById('chat-peer-name').textContent = chat.name;
    document.getElementById('chat-peer-status').textContent = '';
    document.getElementById('chat-page').classList.add('active');
    document.getElementById('btn-send').disabled = false;
    document.getElementById('chat-input').disabled = false;
    
    this.renderChatMessages(peerId);
  }

  closeChat() {
    this.currentChat = null;
    document.getElementById('chat-page').classList.remove('active');
  }

  renderChatMessages(peerId) {
    const container = document.getElementById('chat-messages');
    const chat = this.chatHistory.get(peerId);
    if (!chat) return;

    container.innerHTML = chat.messages.map(msg => this.createMessageHTML(msg)).join('');
    container.scrollTop = container.scrollHeight;

    container.querySelectorAll('.image-msg img').forEach(img => {
      img.addEventListener('click', () => this.showMedia(img.src, 'image'));
    });
  }

  createMessageHTML(msg) {
    const isSent = msg.from === this.p2p.getMyId();
    const time = new Date(msg.timestamp).toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });
    
    if (msg.contentType === 'text') {
      return `
        <div class="chat-msg ${isSent ? 'sent' : ''}">
          <div class="chat-msg-avatar">${isSent ? '我' : (this.chatHistory.get(msg.from)?.avatar || '?')}</div>
          <div class="chat-msg-content">
            <div class="chat-msg-bubble">${this.escape(msg.content)}</div>
            <div class="chat-msg-time">${time}</div>
          </div>
        </div>
      `;
    }
    
    if (msg.contentType === 'image') {
      const dataUrl = `data:${msg.content.mimeType};base64,${msg.content.data}`;
      return `
        <div class="chat-msg ${isSent ? 'sent' : ''}">
          <div class="chat-msg-avatar">${isSent ? '我' : (this.chatHistory.get(msg.from)?.avatar || '?')}</div>
          <div class="chat-msg-content">
            <div class="chat-msg-bubble image-msg">
              <img src="${dataUrl}" alt="${msg.content.name}">
            </div>
            <div class="chat-msg-time">${time}</div>
          </div>
        </div>
      `;
    }
    
    if (msg.contentType === 'file') {
      return `
        <div class="chat-msg ${isSent ? 'sent' : ''}">
          <div class="chat-msg-avatar">${isSent ? '我' : (this.chatHistory.get(msg.from)?.avatar || '?')}</div>
          <div class="chat-msg-content">
            <div class="chat-msg-bubble file-msg">
              <div class="file-icon">${this.getFileIcon(msg.content.mimeType)}</div>
              <div class="file-name">${this.escape(msg.content.name)}</div>
              <div class="file-size">${this.formatSize(msg.content.size)}</div>
              ${msg.content.data ? `<button class="btn btn-primary" style="margin-top:8px;padding:6px 12px;font-size:12px" onclick="window.ppx.saveFile('${msg.id}')">保存</button>` : ''}
            </div>
            <div class="chat-msg-time">${time}</div>
          </div>
        </div>
      `;
    }
    
    return '';
  }

  handleMessage(msg) {
    const peerId = msg.from;
    let chat = this.chatHistory.get(peerId);
    
    if (!chat) {
      const peer = this.p2p.getPeer(peerId);
      chat = {
        id: peerId,
        name: peer?.username || '未知用户',
        avatar: (peer?.username || '?').substring(0, 1),
        messages: [],
        lastTime: 0,
        lastMsg: ''
      };
      this.chatHistory.set(peerId, chat);
    }
    
    chat.messages.push(msg);
    chat.lastTime = msg.timestamp;
    chat.lastMsg = msg.contentType === 'text' ? msg.content : `[${msg.contentType === 'image' ? '图片' : '文件'}]`;
    
    if (this.currentChat === peerId) {
      this.renderChatMessages(peerId);
      this.p2p.sendReadReceipt(peerId, msg.id);
    } else {
      this.incrementUnread(peerId);
      this.notify(`新消息 from ${chat.name}`, msg.contentType === 'text' ? msg.content.substring(0, 50) : `[${msg.contentType}]`);
    }
    
    if (this.currentTab === 'chats') this.renderChatList();
  }

  showTyping(data) {
    if (data.from === this.currentChat) {
      document.getElementById('typing-hint').style.display = 'block';
      clearTimeout(this.typingTimer);
      this.typingTimer = setTimeout(() => {
        document.getElementById('typing-hint').style.display = 'none';
      }, 3000);
    }
  }

  setupChat() {
    const input = document.getElementById('chat-input');
    const sendBtn = document.getElementById('btn-send');

    input.addEventListener('input', () => {
      sendBtn.disabled = !input.value.trim();
      if (this.currentChat) {
        this.p2p.sendTyping(this.currentChat);
      }
    });

    input.addEventListener('keypress', e => {
      if (e.key === 'Enter' && input.value.trim()) {
        this.sendMessage();
      }
    });

    sendBtn.addEventListener('click', () => this.sendMessage());

    document.getElementById('btn-back').addEventListener('click', () => this.closeChat());

    document.getElementById('btn-attach').addEventListener('click', () => {
      document.getElementById('file-input').click();
    });

    document.getElementById('file-input').addEventListener('change', e => this.handleFileSelect(e));

    document.getElementById('btn-chat-menu').addEventListener('click', e => {
      document.querySelector('.chat-page-menu').classList.toggle('show');
      e.stopPropagation();
    });

    document.addEventListener('click', () => {
      document.querySelector('.chat-page-menu')?.classList.remove('show');
    });
  }

  sendMessage() {
    const input = document.getElementById('chat-input');
    const content = input.value.trim();
    if (!content || !this.currentChat) return;

    this.p2p.sendMessage(this.currentChat, content, 'text');
    input.value = '';
    document.getElementById('btn-send').disabled = true;
    this.renderChatMessages(this.currentChat);
  }

  handleFileSelect(e) {
    const files = Array.from(e.target.files);
    if (!files.length || !this.currentChat) return;

    files.forEach(file => {
      const reader = new FileReader();
      reader.onload = ev => {
        const base64 = ev.target.result.split(',')[1];
        const content = {
          name: file.name,
          size: file.size,
          mimeType: file.type,
          data: base64
        };

        this.p2p.sendMessage(this.currentChat, content, file.type.startsWith('image/') ? 'image' : 'file');
        this.renderChatMessages(this.currentChat);
        
        this.files.unshift({
          id: Date.now(),
          name: file.name,
          size: file.size,
          type: file.type,
          data: base64,
          time: Date.now(),
          from: this.currentChat
        });
      };
      reader.readAsDataURL(file);
    });

    e.target.value = '';
  }

  saveFile(msgId) {
    for (const [, chat] of this.chatHistory) {
      const msg = chat.messages.find(m => m.id === msgId);
      if (msg && msg.content.data) {
        const mimeType = msg.content.mimeType || 'application/octet-stream';
        const blob = this.base64ToBlob(msg.content.data, mimeType);
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = msg.content.name;
        a.click();
        URL.revokeObjectURL(url);
        this.showToast('已保存');
        return;
      }
    }
  }

  showFileOffer(offer) {
    this.currentFileOffer = offer;
    document.getElementById('modal-file-name').textContent = offer.file.name;
    document.getElementById('modal-file-size').textContent = this.formatSize(offer.file.size);
    document.getElementById('modal-file-from').textContent = offer.fromName || '未知';
    document.getElementById('download-modal').style.display = 'flex';
  }

  setupDownloads() {
    document.getElementById('btn-downloads-clear').addEventListener('click', () => {
      this.showConfirm('确定清空已完成任务？', () => {
        for (const [id, dl] of this.downloads) {
          if (dl.status === 'completed') this.downloads.delete(id);
        }
        this.renderDownloads();
      });
    });
  }

  renderDownloads() {
    const container = document.getElementById('downloads-list');
    const items = Array.from(this.downloads.values()).sort((a, b) => b.time - a.time);

    let downloading = 0, completed = 0, totalSpeed = 0;
    items.forEach(dl => {
      if (dl.status === 'downloading') { downloading++; totalSpeed += dl.speed || 0; }
      else if (dl.status === 'completed') completed++;
    });

    document.getElementById('downloading-count').textContent = downloading;
    document.getElementById('completed-count').textContent = completed;
    document.getElementById('total-speed').textContent = this.formatSpeed(totalSpeed);

    if (items.length === 0) {
      container.innerHTML = this.emptyState('⬇️', '暂无下载记录');
      return;
    }

    container.innerHTML = items.map(dl => `
      <div class="download-item">
        <div class="download-icon">${this.getFileIcon(dl.type)}</div>
        <div class="download-info">
          <div class="download-name">${this.escape(dl.name)}</div>
          <div class="download-progress"><div class="download-progress-bar" style="width:${dl.progress}%"></div></div>
          <div class="download-meta">
            <span class="download-status ${dl.status === 'failed' ? 'failed' : ''}">${dl.status === 'downloading' ? dl.progress + '%' : dl.status === 'completed' ? '已完成' : '失败'}</span>
            <span>${dl.status === 'downloading' ? this.formatSpeed(dl.speed) : this.formatSize(dl.size)}</span>
          </div>
        </div>
        <div class="download-actions">
          ${dl.status === 'downloading' ? '<button class="action-btn" onclick="window.ppx.cancelDownload(\'' + dl.id + '\')">✕</button>' : ''}
          ${dl.status === 'completed' ? '<button class="action-btn" onclick="window.ppx.openFile(\'' + dl.id + '\')">📂</button>' : ''}
        </div>
      </div>
    `).join('');
  }

  addDownload(name, size, type) {
    const id = Date.now().toString();
    this.downloads.set(id, {
      id, name, size, type,
      progress: 0,
      speed: 0,
      status: 'downloading',
      time: Date.now(),
      data: null
    });
    this.renderDownloads();
    return id;
  }

  updateDownload(id, progress, speed) {
    const dl = this.downloads.get(id);
    if (dl) {
      dl.progress = progress;
      dl.speed = speed;
      this.renderDownloads();
    }
  }

  completeDownload(id, data) {
    const dl = this.downloads.get(id);
    if (dl) {
      dl.status = 'completed';
      dl.progress = 100;
      dl.data = data;
      this.renderDownloads();
    }
  }

  failDownload(id) {
    const dl = this.downloads.get(id);
    if (dl) {
      dl.status = 'failed';
      this.renderDownloads();
    }
  }

  cancelDownload(id) {
    this.downloads.delete(id);
    this.renderDownloads();
  }

  openFile(id) {
    const dl = this.downloads.get(id);
    if (dl && dl.data) {
      const blob = this.base64ToBlob(dl.data, dl.type);
      const url = URL.createObjectURL(blob);
      window.open(url, '_blank');
    }
  }

  renderFiles(type) {
    const container = document.getElementById('files-list');
    let filtered = type === 'all' ? this.files : this.files.filter(f => {
      if (type === 'image') return f.type.startsWith('image/');
      if (type === 'video') return f.type.startsWith('video/');
      if (type === 'audio') return f.type.startsWith('audio/');
      if (type === 'doc') return f.type.includes('pdf') || f.type.includes('document') || f.type.includes('text');
      return true;
    });

    if (filtered.length === 0) {
      container.innerHTML = this.emptyState('📁', '暂无文件');
      return;
    }

    const images = filtered.filter(f => f.type.startsWith('image/'));
    const others = filtered.filter(f => !f.type.startsWith('image/'));

    let html = '';
    if (images.length > 0) {
      html += `<div class="file-grid">${images.map(f => `
        <div class="file-grid-item" onclick="window.ppx.previewFile('${f.id}')">
          <img src="data:${f.type};base64,${f.data}" alt="${f.name}">
        </div>
      `).join('')}</div>`;
    }

    if (others.length > 0) {
      html += others.map(f => `
        <div class="file-list-item" onclick="window.ppx.previewFile('${f.id}')">
          <div class="file-icon">${this.getFileIcon(f.type)}</div>
          <div class="file-info">
            <div class="file-name">${this.escape(f.name)}</div>
            <div class="file-meta">${this.formatSize(f.size)} · ${this.formatTime(f.time)}</div>
          </div>
        </div>
      `).join('');
    }

    container.innerHTML = html;
  }

  previewFile(id) {
    const file = this.files.find(f => f.id == id);
    if (!file) return;
    
    if (file.type.startsWith('image/')) {
      this.showMedia(`data:${file.type};base64,${file.data}`, 'image');
    } else if (file.type.startsWith('video/')) {
      this.showMedia(`data:${file.type};base64,${file.data}`, 'video');
    } else if (file.type.startsWith('audio/')) {
      this.showMedia(`data:${file.type};base64,${file.data}`, 'audio');
    } else {
      const blob = this.base64ToBlob(file.data, file.type);
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = file.name;
      a.click();
      URL.revokeObjectURL(url);
    }
  }

  showMedia(src, type) {
    const viewer = document.getElementById('media-viewer');
    const content = document.getElementById('media-content');
    
    if (type === 'image') {
      content.innerHTML = `<img src="${src}" alt="">`;
    } else if (type === 'video') {
      content.innerHTML = `<video src="${src}" controls autoplay style="max-width:100%;max-height:100%"></video>`;
    } else if (type === 'audio') {
      content.innerHTML = `<audio src="${src}" controls autoplay></audio>`;
    }
    
    viewer.style.display = 'flex';
  }

  setupProfile() {
    document.getElementById('menu-edit-name').addEventListener('click', () => {
      this.showInput('修改昵称', localStorage.getItem('ppx_username') || '', val => {
        localStorage.setItem('ppx_username', val);
        document.getElementById('my-name').textContent = val;
        document.getElementById('my-avatar').textContent = val.substring(0, 1);
        this.p2p.updateUsername(val);
        this.showToast('已修改');
      });
    });

    document.getElementById('menu-save-history').addEventListener('click', () => this.saveHistory());
    document.getElementById('menu-share-app').addEventListener('click', () => this.shareApp());
    document.getElementById('menu-about').addEventListener('click', () => this.showAbout());
  }

  saveHistory() {
    const data = {
      exportTime: new Date().toISOString(),
      chats: Array.from(this.chatHistory.entries()).map(([id, chat]) => ({
        peerId: id,
        peerName: chat.name,
        messages: chat.messages
      }))
    };
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `ppx-history-${Date.now()}.json`;
    a.click();
    URL.revokeObjectURL(url);
    this.showToast('已保存');
  }

  shareApp() {
    const url = window.location.href;
    if (navigator.share) {
      navigator.share({ title: 'PPX - 去中心化聊天', url });
    } else {
      navigator.clipboard.writeText(url);
      this.showToast('链接已复制');
    }
  }

  showAbout() {
    this.showConfirm('PPX v1.0\n去中心化P2P聊天应用\n支持文件传输、断点续传', () => {});
  }

  updateConnectionInfo() {
    const indicator = document.getElementById('status-indicator');
    const text = document.getElementById('status-text');
    indicator.classList.add('online');
    text.textContent = '已连接';
    document.getElementById('conn-type').textContent = typeof BroadcastChannel !== 'undefined' ? 'BroadcastChannel' : 'WebRTC';
  }

  setupModals() {
    document.getElementById('btn-close-modal').addEventListener('click', () => {
      document.getElementById('download-modal').style.display = 'none';
    });
    document.getElementById('btn-accept').addEventListener('click', () => this.acceptFile());
    document.getElementById('btn-reject').addEventListener('click', () => this.rejectFile());
    document.getElementById('btn-close-media').addEventListener('click', () => {
      document.getElementById('media-viewer').style.display = 'none';
    });
    document.getElementById('btn-media-save').addEventListener('click', () => {
      document.getElementById('media-viewer').style.display = 'none';
    });

    document.querySelectorAll('.btn-close-input').forEach(btn => {
      btn.addEventListener('click', () => {
        document.getElementById('input-modal').style.display = 'none';
      });
    });
    document.getElementById('btn-input-confirm').addEventListener('click', () => {
      const val = document.getElementById('input-field').value;
      document.getElementById('input-modal').style.display = 'none';
      if (this.inputCallback) this.inputCallback(val);
    });

    document.getElementById('btn-cancel').addEventListener('click', () => {
      document.getElementById('confirm-modal').style.display = 'none';
    });
  }

  acceptFile() {
    document.getElementById('download-modal').style.display = 'none';
    if (!this.currentFileOffer) return;

    const offer = this.currentFileOffer;
    const msg = {
      type: 'chat',
      id: offer.id,
      from: offer.from,
      contentType: offer.file.mimeType?.startsWith('image/') ? 'image' : 'file',
      content: offer.file,
      timestamp: Date.now()
    };

    const chat = this.chatHistory.get(offer.from);
    if (chat) chat.messages.push(msg);
    if (this.currentChat === offer.from) this.renderChatMessages(offer.from);
    
    this.p2p.respondToFileOffer(offer.id, true, offer.from);
    this.showToast('开始接收文件');
    this.currentFileOffer = null;
  }

  rejectFile() {
    document.getElementById('download-modal').style.display = 'none';
    if (this.currentFileOffer) {
      this.p2p.respondToFileOffer(this.currentFileOffer.id, false, this.currentFileOffer.from);
    }
    this.currentFileOffer = null;
  }

  showInput(title, defaultVal, callback) {
    document.getElementById('input-title').textContent = title;
    document.getElementById('input-field').value = defaultVal;
    document.getElementById('input-modal').style.display = 'flex';
    this.inputCallback = callback;
    document.getElementById('input-field').focus();
  }

  showConfirm(text, callback) {
    document.getElementById('confirm-text').textContent = text;
    document.getElementById('confirm-modal').style.display = 'flex';
    document.getElementById('btn-confirm').onclick = () => {
      document.getElementById('confirm-modal').style.display = 'none';
      callback();
    };
  }

  showToast(text) {
    const toast = document.getElementById('toast');
    document.getElementById('toast-text').textContent = text;
    toast.style.display = 'block';
    setTimeout(() => toast.style.display = 'none', 2000);
  }

  notify(title, body) {
    if (Notification.permission === 'granted') {
      new Notification(title, { body });
    }
  }

  clearUnread(peerId) {
    this.unreadCount.set(peerId, 0);
    this.updateUnreadBadge();
    this.renderChatList();
  }

  incrementUnread(peerId) {
    const count = (this.unreadCount.get(peerId) || 0) + 1;
    this.unreadCount.set(peerId, count);
    this.updateUnreadBadge();
  }

  updateUnreadBadge() {
    let total = 0;
    for (const count of this.unreadCount.values()) total += count;
    const badge = document.getElementById('unread-badge');
    if (total > 0) {
      badge.textContent = total > 99 ? '99+' : total;
      badge.style.display = 'flex';
    } else {
      badge.style.display = 'none';
    }
  }

  emptyState(icon, text) {
    return `<div class="empty-state"><div class="empty-icon">${icon}</div><div class="empty-text">${text}</div></div>`;
  }

  getPreview(msg) {
    if (!msg) return '';
    return msg.length > 40 ? msg.substring(0, 40) + '...' : msg;
  }

  formatTime(ts) {
    if (!ts) return '';
    const d = new Date(ts);
    const now = new Date();
    if (d.toDateString() === now.toDateString()) {
      return d.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });
    }
    return d.toLocaleDateString('zh-CN', { month: 'short', day: 'numeric' });
  }

  formatSize(bytes) {
    if (!bytes) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
  }

  formatSpeed(bytesPerSec) {
    return this.formatSize(bytesPerSec) + '/s';
  }

  getFileIcon(mimeType) {
    if (!mimeType) return '📄';
    if (mimeType.startsWith('image/')) return '🖼️';
    if (mimeType.startsWith('video/')) return '🎬';
    if (mimeType.startsWith('audio/')) return '🎵';
    if (mimeType.includes('pdf')) return '📕';
    if (mimeType.includes('word') || mimeType.includes('document')) return '📘';
    if (mimeType.includes('sheet') || mimeType.includes('excel')) return '📗';
    if (mimeType.includes('zip') || mimeType.includes('archive')) return '📦';
    return '📄';
  }

  escape(str) {
    if (typeof str !== 'string') return '';
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
  }

  base64ToBlob(base64, mimeType) {
    const binary = atob(base64);
    const len = binary.length;
    const bytes = new Uint8Array(len);
    for (let i = 0; i < len; i++) bytes[i] = binary.charCodeAt(i);
    return new Blob([bytes], { type: mimeType });
  }
}

document.addEventListener('DOMContentLoaded', () => {
  window.ppx = new PPXApp();
});
