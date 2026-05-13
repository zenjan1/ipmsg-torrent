import { P2PChat } from './core/p2p.js';

const STORAGE_KEY_CHAT = 'ppx_chat_history';
const STORAGE_KEY_FILES = 'ppx_files';
const STORAGE_KEY_UNREAD = 'ppx_unread';
const STORAGE_KEY_DARK = 'ppx_dark_mode';
const STORAGE_KEY_USERNAME = 'ppx_username';
const RECONNECT_DELAY = 5000;
const MAX_RECONNECT_ATTEMPTS = 10;

const EMOJI_LIST = ['😀','😂','🤣','😊','😍','🥰','😘','😜','🤔','😅','😢','😡','👍','👎','👏','🙌','💪','🎉','🔥','⭐','❤️','💔','🎂','🍕','☕','🌞','🌈','🐱','🐶','🦊'];

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
    this.searchTerm = '';
    this.reconnectAttempts = new Map();
    this.reconnectTimers = new Map();
    this.connectionQuality = 'unknown';
    this.qualityCheckTimer = null;
    this.inputCallback = null;
    this.emojiVisible = false;

    this.init();
  }

  init() {
    try {
      this.loadFromStorage();
      this.applyTheme();

      const defaultName = localStorage.getItem(STORAGE_KEY_USERNAME) || `用户${Math.floor(Math.random() * 9999)}`;

      this.p2p = new P2PChat();
      this.setupTabs();
      this.setupChat();
      this.setupDownloads();
      this.setupProfile();
      this.setupModals();
      this.setupSearch();
      this.setupEmojiPicker();
      this.setupDarkModeToggle();

      this.requestNotificationPermission();

      this.p2p.init(defaultName).then(info => {
        try {
          document.getElementById('my-name').textContent = info.username;
          document.getElementById('my-id').textContent = info.id.substring(0, 8);
          document.getElementById('my-avatar').textContent = info.username.substring(0, 1);
          this.updateConnectionInfo();
          this.startQualityMonitor();
          this.restoreReconnectTargets();
        } catch (e) {
          console.error('Init UI update failed:', e);
        }
      }).catch(err => {
        console.error('P2P init failed:', err);
        this.showToast('连接初始化失败，请刷新页面重试');
      });

      this.p2p.on('peers-updated', peers => {
        try { this.renderPeers(peers); } catch (e) { console.error('peers-updated handler:', e); }
      });
      this.p2p.on('peer-joined', peer => {
        try {
          this.showToast(`${peer.username} 上线`);
          this.clearReconnect(peer.id);
        } catch (e) { console.error('peer-joined handler:', e); }
      });
      this.p2p.on('peer-left', peer => {
        try {
          this.showToast(`${peer.username} 离线`);
          if (this.currentChat === peer.id) this.closeChat();
          this.scheduleReconnect(peer);
        } catch (e) { console.error('peer-left handler:', e); }
      });
      this.p2p.on('message', msg => {
        try { this.handleMessage(msg); } catch (e) { console.error('message handler:', e); }
      });
      this.p2p.on('typing', data => {
        try { this.showTyping(data); } catch (e) { console.error('typing handler:', e); }
      });
      this.p2p.on('file-offer', offer => {
        try { this.showFileOffer(offer); } catch (e) { console.error('file-offer handler:', e); }
      });
      this.p2p.on('connection-quality', quality => {
        try { this.updateConnectionQuality(quality); } catch (e) { console.error('quality handler:', e); }
      });
    } catch (e) {
      console.error('App init failed:', e);
    }
  }

  loadFromStorage() {
    try {
      const chatData = localStorage.getItem(STORAGE_KEY_CHAT);
      if (chatData) {
        const parsed = JSON.parse(chatData);
        this.chatHistory = new Map(parsed);
      }
    } catch (e) {
      console.error('Failed to load chat history:', e);
    }

    try {
      const filesData = localStorage.getItem(STORAGE_KEY_FILES);
      if (filesData) {
        this.files = JSON.parse(filesData);
      }
    } catch (e) {
      console.error('Failed to load files:', e);
    }

    try {
      const unreadData = localStorage.getItem(STORAGE_KEY_UNREAD);
      if (unreadData) {
        this.unreadCount = new Map(JSON.parse(unreadData));
      }
    } catch (e) {
      console.error('Failed to load unread counts:', e);
    }
  }

  persistChatHistory() {
    try {
      localStorage.setItem(STORAGE_KEY_CHAT, JSON.stringify(Array.from(this.chatHistory.entries())));
    } catch (e) {
      console.error('Failed to persist chat history:', e);
    }
  }

  persistFiles() {
    try {
      localStorage.setItem(STORAGE_KEY_FILES, JSON.stringify(this.files.slice(0, 200)));
    } catch (e) {
      console.error('Failed to persist files:', e);
    }
  }

  persistUnread() {
    try {
      localStorage.setItem(STORAGE_KEY_UNREAD, JSON.stringify(Array.from(this.unreadCount.entries())));
    } catch (e) {
      console.error('Failed to persist unread counts:', e);
    }
  }

  requestNotificationPermission() {
    try {
      if ('Notification' in window && Notification.permission === 'default') {
        Notification.requestPermission().catch(() => {});
      }
    } catch (e) {
      console.error('Notification request failed:', e);
    }
  }

  applyTheme() {
    try {
      const isDark = localStorage.getItem(STORAGE_KEY_DARK) === 'true';
      if (isDark) {
        document.body.classList.add('dark');
      } else {
        document.body.classList.remove('dark');
      }
      this.updateDarkModeButton(isDark);
    } catch (e) {
      console.error('Failed to apply theme:', e);
    }
  }

  toggleDarkMode() {
    try {
      const isDark = document.body.classList.toggle('dark');
      localStorage.setItem(STORAGE_KEY_DARK, isDark.toString());
      this.updateDarkModeButton(isDark);
      this.showToast(isDark ? '已切换为深色模式' : '已切换为浅色模式');
    } catch (e) {
      console.error('Failed to toggle dark mode:', e);
    }
  }

  updateDarkModeButton(isDark) {
    const btn = document.getElementById('menu-dark-mode');
    if (btn) {
      btn.textContent = isDark ? '☀️ 浅色模式' : '🌙 深色模式';
    }
  }

  setupDarkModeToggle() {
    const btn = document.getElementById('menu-dark-mode');
    if (btn) {
      btn.addEventListener('click', () => this.toggleDarkMode());
    }
  }

  setupSearch() {
    const searchInput = document.getElementById('chat-search-input');
    if (!searchInput) return;

    let searchTimer = null;
    searchInput.addEventListener('input', () => {
      clearTimeout(searchTimer);
      searchTimer = setTimeout(() => {
        this.searchTerm = searchInput.value.trim().toLowerCase();
        if (this.currentChat) {
          this.renderChatMessages(this.currentChat);
        }
      }, 300);
    });

    document.getElementById('btn-search-clear')?.addEventListener('click', () => {
      searchInput.value = '';
      this.searchTerm = '';
      if (this.currentChat) {
        this.renderChatMessages(this.currentChat);
      }
    });
  }

  setupEmojiPicker() {
    const btn = document.getElementById('btn-emoji');
    const panel = document.getElementById('emoji-panel');
    if (!btn || !panel) return;

    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      this.emojiVisible = !this.emojiVisible;
      panel.style.display = this.emojiVisible ? 'flex' : 'none';
    });

    panel.innerHTML = EMOJI_LIST.map(emoji => 
      `<span class="emoji-item" data-emoji="${emoji}">${emoji}</span>`
    ).join('');

    panel.querySelectorAll('.emoji-item').forEach(item => {
      item.addEventListener('click', () => {
        const input = document.getElementById('chat-input');
        input.value += item.dataset.emoji;
        input.focus();
        document.getElementById('btn-send').disabled = false;
        this.emojiVisible = false;
        panel.style.display = 'none';
      });
    });

    document.addEventListener('click', (e) => {
      if (!panel.contains(e.target) && e.target !== btn) {
        this.emojiVisible = false;
        panel.style.display = 'none';
      }
    });
  }

  startQualityMonitor() {
    this.qualityCheckTimer = setInterval(() => {
      try {
        if (this.p2p && this.p2p.getPeers) {
          const peers = this.p2p.getPeers();
          if (peers.length > 0) {
            this.updateConnectionQuality('good');
          }
        }
      } catch (e) {
        this.updateConnectionQuality('poor');
      }
    }, 10000);
  }

  updateConnectionQuality(quality) {
    this.connectionQuality = quality;
    const indicator = document.getElementById('status-indicator');
    const text = document.getElementById('status-text');
    if (!indicator || !text) return;

    indicator.classList.remove('online', 'quality-good', 'quality-poor', 'quality-lost');
    
    switch (quality) {
      case 'excellent':
        indicator.classList.add('quality-good');
        text.textContent = '连接良好';
        break;
      case 'good':
        indicator.classList.add('online');
        text.textContent = '已连接';
        break;
      case 'poor':
        indicator.classList.add('quality-poor');
        text.textContent = '连接较弱';
        break;
      case 'lost':
        indicator.classList.add('quality-lost');
        text.textContent = '连接断开';
        break;
      default:
        indicator.classList.add('online');
        text.textContent = '已连接';
    }
  }

  restoreReconnectTargets() {
    try {
      const stored = localStorage.getItem('ppx_reconnect_peers');
      if (stored) {
        const peers = JSON.parse(stored);
        peers.forEach(p => this.scheduleReconnect(p));
        localStorage.removeItem('ppx_reconnect_peers');
      }
    } catch (e) {
      console.error('Failed to restore reconnect targets:', e);
    }
  }

  persistReconnectTargets() {
    try {
      const targets = Array.from(this.reconnectAttempts.keys());
      if (targets.length > 0) {
        localStorage.setItem('ppx_reconnect_peers', JSON.stringify(targets.map(id => ({ id }))));
      }
    } catch (e) {
      console.error('Failed to persist reconnect targets:', e);
    }
  }

  scheduleReconnect(peer) {
    try {
      const peerId = peer.id || peer;
      const attempts = (this.reconnectAttempts.get(peerId) || 0);
      
      if (attempts >= MAX_RECONNECT_ATTEMPTS) {
        this.reconnectAttempts.delete(peerId);
        this.showToast(`无法重连到 ${peer.username || peerId}`);
        return;
      }

      if (this.reconnectTimers.has(peerId)) {
        clearTimeout(this.reconnectTimers.get(peerId));
      }

      const delay = RECONNECT_DELAY * Math.pow(1.5, attempts);
      this.reconnectAttempts.set(peerId, attempts + 1);
      this.persistReconnectTargets();

      const timer = setTimeout(() => {
        try {
          if (this.p2p && this.p2p.reconnectToPeer) {
            this.p2p.reconnectToPeer(peerId);
          }
        } catch (e) {
          console.error('Reconnect attempt failed:', e);
        }
        this.reconnectTimers.delete(peerId);
      }, delay);

      this.reconnectTimers.set(peerId, timer);
    } catch (e) {
      console.error('Failed to schedule reconnect:', e);
    }
  }

  clearReconnect(peerId) {
    try {
      this.reconnectAttempts.delete(peerId);
      if (this.reconnectTimers.has(peerId)) {
        clearTimeout(this.reconnectTimers.get(peerId));
        this.reconnectTimers.delete(peerId);
      }
    } catch (e) {
      console.error('Failed to clear reconnect:', e);
    }
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
    const targetTab = document.querySelector(`.tab-item[data-tab="${name}"]`);
    if (targetTab) targetTab.classList.add('active');
    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
    const targetContent = document.getElementById(`${name}-tab`);
    if (targetContent) targetContent.classList.add('active');

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
    const countEl = document.getElementById('online-count');
    if (countEl) countEl.textContent = peers.length;

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
    if (!container) return;

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

    const nameEl = document.getElementById('chat-peer-name');
    const statusEl = document.getElementById('chat-peer-status');
    const page = document.getElementById('chat-page');
    const sendBtn = document.getElementById('btn-send');
    const input = document.getElementById('chat-input');

    if (nameEl) nameEl.textContent = chat.name;
    if (statusEl) statusEl.textContent = '';
    if (page) page.classList.add('active');
    if (sendBtn) sendBtn.disabled = false;
    if (input) input.disabled = false;

    const searchInput = document.getElementById('chat-search-input');
    if (searchInput) {
      searchInput.value = '';
      this.searchTerm = '';
    }

    this.renderChatMessages(peerId);
  }

  closeChat() {
    this.currentChat = null;
    const page = document.getElementById('chat-page');
    if (page) page.classList.remove('active');
  }

  renderChatMessages(peerId) {
    const container = document.getElementById('chat-messages');
    const chat = this.chatHistory.get(peerId);
    if (!chat || !container) return;

    let messages = chat.messages;
    if (this.searchTerm) {
      messages = messages.filter(msg => {
        if (msg.contentType === 'text') {
          return msg.content.toLowerCase().includes(this.searchTerm);
        }
        if (msg.content && msg.content.name) {
          return msg.content.name.toLowerCase().includes(this.searchTerm);
        }
        return false;
      });
    }

    container.innerHTML = messages.map(msg => this.createMessageHTML(msg)).join('');
    container.scrollTop = container.scrollHeight;

    container.querySelectorAll('.image-msg img').forEach(img => {
      img.addEventListener('click', () => {
        try { this.showMedia(img.src, 'image'); } catch (e) { console.error('Show media failed:', e); }
      });
    });
  }

  createMessageHTML(msg) {
    const isSent = msg.from === (this.p2p ? this.p2p.getMyId() : '');
    const time = new Date(msg.timestamp).toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });

    const highlightText = (text) => {
      if (!this.searchTerm || typeof text !== 'string') return this.escape(text);
      const escaped = this.escape(text);
      const searchEscaped = this.escape(this.searchTerm);
      const regex = new RegExp(`(${searchEscaped.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi');
      return escaped.replace(regex, '<mark class="search-highlight">$1</mark>');
    };

    if (msg.contentType === 'text') {
      return `
        <div class="chat-msg ${isSent ? 'sent' : ''}">
          <div class="chat-msg-avatar">${isSent ? '我' : (this.chatHistory.get(msg.from)?.avatar || '?')}</div>
          <div class="chat-msg-content">
            <div class="chat-msg-bubble">${highlightText(msg.content)}</div>
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
              <img src="${dataUrl}" alt="${this.escape(msg.content.name)}" loading="lazy">
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
              <div class="file-name">${highlightText(msg.content.name)}</div>
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
      let peer = null;
      try { peer = this.p2p.getPeer(peerId); } catch (e) { console.error('Get peer failed:', e); }
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

    this.persistChatHistory();

    if (this.currentChat === peerId) {
      this.renderChatMessages(peerId);
      try { this.p2p.sendReadReceipt(peerId, msg.id); } catch (e) { console.error('Send read receipt failed:', e); }
    } else {
      this.incrementUnread(peerId);
      this.notify(`新消息 from ${chat.name}`, msg.contentType === 'text' ? msg.content.substring(0, 50) : `[${msg.contentType}]`);
    }

    if (this.currentTab === 'chats') this.renderChatList();
  }

  showTyping(data) {
    if (data.from === this.currentChat) {
      const hint = document.getElementById('typing-hint');
      if (hint) hint.style.display = 'block';
      clearTimeout(this.typingTimer);
      this.typingTimer = setTimeout(() => {
        const hintEl = document.getElementById('typing-hint');
        if (hintEl) hintEl.style.display = 'none';
      }, 3000);
    }
  }

  setupChat() {
    const input = document.getElementById('chat-input');
    const sendBtn = document.getElementById('btn-send');

    if (input) {
      input.addEventListener('input', () => {
        if (sendBtn) sendBtn.disabled = !input.value.trim();
        if (this.currentChat) {
          try { this.p2p.sendTyping(this.currentChat); } catch (e) { console.error('Send typing failed:', e); }
        }
      });

      input.addEventListener('keypress', e => {
        if (e.key === 'Enter' && input.value.trim()) {
          this.sendMessage();
        }
      });
    }

    if (sendBtn) {
      sendBtn.addEventListener('click', () => this.sendMessage());
    }

    const backBtn = document.getElementById('btn-back');
    if (backBtn) backBtn.addEventListener('click', () => this.closeChat());

    const attachBtn = document.getElementById('btn-attach');
    if (attachBtn) {
      attachBtn.addEventListener('click', () => {
        const fileInput = document.getElementById('file-input');
        if (fileInput) fileInput.click();
      });
    }

    const fileInput = document.getElementById('file-input');
    if (fileInput) {
      fileInput.setAttribute('multiple', '');
      fileInput.addEventListener('change', e => this.handleFileSelect(e));
    }

    const menuBtn = document.getElementById('btn-chat-menu');
    if (menuBtn) {
      menuBtn.addEventListener('click', e => {
        const menu = document.querySelector('.chat-page-menu');
        if (menu) menu.classList.toggle('show');
        e.stopPropagation();
      });
    }

    document.addEventListener('click', () => {
      const menu = document.querySelector('.chat-page-menu');
      if (menu) menu.classList.remove('show');
    });
  }

  sendMessage() {
    try {
      const input = document.getElementById('chat-input');
      if (!input) return;
      const content = input.value.trim();
      if (!content || !this.currentChat) return;

      this.p2p.sendMessage(this.currentChat, content, 'text');
      input.value = '';
      const sendBtn = document.getElementById('btn-send');
      if (sendBtn) sendBtn.disabled = true;
      this.renderChatMessages(this.currentChat);
      this.persistChatHistory();
    } catch (e) {
      console.error('Send message failed:', e);
      this.showToast('发送失败，请重试');
    }
  }

  handleFileSelect(e) {
    try {
      const files = Array.from(e.target.files);
      if (!files.length || !this.currentChat) return;

      this.showToast(`正在发送 ${files.length} 个文件...`);

      files.forEach(file => {
        if (file.size > 50 * 1024 * 1024) {
          this.showToast(`${file.name} 超过50MB限制，已跳过`);
          return;
        }

        try {
          const reader = new FileReader();
          reader.onload = ev => {
            try {
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
                id: Date.now() + Math.random(),
                name: file.name,
                size: file.size,
                type: file.type,
                data: base64,
                time: Date.now(),
                from: this.currentChat
              });
              this.persistFiles();
              this.persistChatHistory();
            } catch (err) {
              console.error('File read callback failed:', err);
              this.showToast(`处理 ${file.name} 失败`);
            }
          };
          reader.onerror = () => {
            this.showToast(`读取 ${file.name} 失败`);
          };
          reader.readAsDataURL(file);
        } catch (err) {
          console.error('File read setup failed:', err);
        }
      });

      e.target.value = '';
    } catch (e) {
      console.error('Handle file select failed:', e);
      this.showToast('文件选择处理失败');
    }
  }

  saveFile(msgId) {
    try {
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
    } catch (e) {
      console.error('Save file failed:', e);
      this.showToast('保存失败');
    }
  }

  showFileOffer(offer) {
    this.currentFileOffer = offer;
    const nameEl = document.getElementById('modal-file-name');
    const sizeEl = document.getElementById('modal-file-size');
    const fromEl = document.getElementById('modal-file-from');
    const modal = document.getElementById('download-modal');
    if (nameEl) nameEl.textContent = offer.file.name;
    if (sizeEl) sizeEl.textContent = this.formatSize(offer.file.size);
    if (fromEl) fromEl.textContent = offer.fromName || '未知';
    if (modal) modal.style.display = 'flex';
  }

  setupDownloads() {
    const clearBtn = document.getElementById('btn-downloads-clear');
    if (clearBtn) {
      clearBtn.addEventListener('click', () => {
        this.showConfirm('确定清空已完成任务？', () => {
          for (const [id, dl] of this.downloads) {
            if (dl.status === 'completed') this.downloads.delete(id);
          }
          this.renderDownloads();
        });
      });
    }
  }

  renderDownloads() {
    const container = document.getElementById('downloads-list');
    if (!container) return;

    const items = Array.from(this.downloads.values()).sort((a, b) => b.time - a.time);

    let downloading = 0, completed = 0, totalSpeed = 0;
    items.forEach(dl => {
      if (dl.status === 'downloading') { downloading++; totalSpeed += dl.speed || 0; }
      else if (dl.status === 'completed') completed++;
    });

    const downCountEl = document.getElementById('downloading-count');
    const compCountEl = document.getElementById('completed-count');
    const speedEl = document.getElementById('total-speed');
    if (downCountEl) downCountEl.textContent = downloading;
    if (compCountEl) compCountEl.textContent = completed;
    if (speedEl) speedEl.textContent = this.formatSpeed(totalSpeed);

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
          ${dl.status === 'downloading' ? `<button class="action-btn" onclick="window.ppx.cancelDownload('${dl.id}')">✕</button>` : ''}
          ${dl.status === 'completed' ? `<button class="action-btn" onclick="window.ppx.openFile('${dl.id}')">📂</button>` : ''}
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
    try {
      const dl = this.downloads.get(id);
      if (dl && dl.data) {
        const blob = this.base64ToBlob(dl.data, dl.type);
        const url = URL.createObjectURL(blob);
        window.open(url, '_blank');
      }
    } catch (e) {
      console.error('Open file failed:', e);
      this.showToast('打开文件失败');
    }
  }

  renderFiles(type) {
    const container = document.getElementById('files-list');
    if (!container) return;

    let filtered = type === 'all' ? this.files : this.files.filter(f => {
      if (type === 'image') return f.type && f.type.startsWith('image/');
      if (type === 'video') return f.type && f.type.startsWith('video/');
      if (type === 'audio') return f.type && f.type.startsWith('audio/');
      if (type === 'doc') return f.type && (f.type.includes('pdf') || f.type.includes('document') || f.type.includes('text'));
      return true;
    });

    if (filtered.length === 0) {
      container.innerHTML = this.emptyState('📁', '暂无文件');
      return;
    }

    const images = filtered.filter(f => f.type && f.type.startsWith('image/'));
    const others = filtered.filter(f => !f.type || !f.type.startsWith('image/'));

    let html = '';
    if (images.length > 0) {
      html += `<div class="file-grid">${images.map(f => `
        <div class="file-grid-item" onclick="window.ppx.previewFile('${f.id}')">
          <img src="data:${f.type};base64,${f.data}" alt="${this.escape(f.name)}" loading="lazy">
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
    try {
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
    } catch (e) {
      console.error('Preview file failed:', e);
      this.showToast('预览失败');
    }
  }

  showMedia(src, type) {
    const viewer = document.getElementById('media-viewer');
    const content = document.getElementById('media-content');
    if (!viewer || !content) return;

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
    const editNameBtn = document.getElementById('menu-edit-name');
    if (editNameBtn) {
      editNameBtn.addEventListener('click', () => {
        this.showInput('修改昵称', localStorage.getItem(STORAGE_KEY_USERNAME) || '', val => {
          try {
            localStorage.setItem(STORAGE_KEY_USERNAME, val);
            const nameEl = document.getElementById('my-name');
            const avatarEl = document.getElementById('my-avatar');
            if (nameEl) nameEl.textContent = val;
            if (avatarEl) avatarEl.textContent = val.substring(0, 1);
            this.p2p.updateUsername(val);
            this.showToast('已修改');
          } catch (e) {
            console.error('Update username failed:', e);
            this.showToast('修改失败');
          }
        });
      });
    }

    const saveHistoryBtn = document.getElementById('menu-save-history');
    if (saveHistoryBtn) {
      saveHistoryBtn.addEventListener('click', () => this.saveHistory());
    }

    const shareBtn = document.getElementById('menu-share-app');
    if (shareBtn) {
      shareBtn.addEventListener('click', () => this.shareApp());
    }

    const aboutBtn = document.getElementById('menu-about');
    if (aboutBtn) {
      aboutBtn.addEventListener('click', () => this.showAbout());
    }
  }

  saveHistory() {
    try {
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
    } catch (e) {
      console.error('Save history failed:', e);
      this.showToast('保存历史记录失败');
    }
  }

  shareApp() {
    try {
      const url = window.location.href;
      if (navigator.share) {
        navigator.share({ title: 'PPX - 去中心化聊天', url }).catch(() => {});
      } else {
        navigator.clipboard.writeText(url).then(() => {
          this.showToast('链接已复制');
        }).catch(() => {
          this.showToast('复制失败');
        });
      }
    } catch (e) {
      console.error('Share app failed:', e);
    }
  }

  showAbout() {
    this.showConfirm('PPX v1.0\n去中心化P2P聊天应用\n支持文件传输、断点续传', () => {});
  }

  updateConnectionInfo() {
    const indicator = document.getElementById('status-indicator');
    const text = document.getElementById('status-text');
    const connType = document.getElementById('conn-type');
    if (indicator) indicator.classList.add('online');
    if (text) text.textContent = '已连接';
    if (connType) {
      try {
        connType.textContent = typeof BroadcastChannel !== 'undefined' ? 'BroadcastChannel' : 'WebRTC';
      } catch (e) {
        connType.textContent = 'WebRTC';
      }
    }
  }

  setupModals() {
    const closeModalBtn = document.getElementById('btn-close-modal');
    if (closeModalBtn) {
      closeModalBtn.addEventListener('click', () => {
        const modal = document.getElementById('download-modal');
        if (modal) modal.style.display = 'none';
      });
    }

    const acceptBtn = document.getElementById('btn-accept');
    if (acceptBtn) acceptBtn.addEventListener('click', () => this.acceptFile());

    const rejectBtn = document.getElementById('btn-reject');
    if (rejectBtn) rejectBtn.addEventListener('click', () => this.rejectFile());

    const closeMediaBtn = document.getElementById('btn-close-media');
    if (closeMediaBtn) {
      closeMediaBtn.addEventListener('click', () => {
        const viewer = document.getElementById('media-viewer');
        if (viewer) viewer.style.display = 'none';
      });
    }

    const mediaSaveBtn = document.getElementById('btn-media-save');
    if (mediaSaveBtn) {
      mediaSaveBtn.addEventListener('click', () => {
        const viewer = document.getElementById('media-viewer');
        if (viewer) viewer.style.display = 'none';
      });
    }

    document.querySelectorAll('.btn-close-input').forEach(btn => {
      btn.addEventListener('click', () => {
        const modal = document.getElementById('input-modal');
        if (modal) modal.style.display = 'none';
      });
    });

    const inputConfirmBtn = document.getElementById('btn-input-confirm');
    if (inputConfirmBtn) {
      inputConfirmBtn.addEventListener('click', () => {
        const val = document.getElementById('input-field')?.value || '';
        const modal = document.getElementById('input-modal');
        if (modal) modal.style.display = 'none';
        if (this.inputCallback) {
          try { this.inputCallback(val); } catch (e) { console.error('Input callback failed:', e); }
        }
      });
    }

    const cancelBtn = document.getElementById('btn-cancel');
    if (cancelBtn) {
      cancelBtn.addEventListener('click', () => {
        const modal = document.getElementById('confirm-modal');
        if (modal) modal.style.display = 'none';
      });
    }
  }

  acceptFile() {
    try {
      const modal = document.getElementById('download-modal');
      if (modal) modal.style.display = 'none';
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
      this.persistChatHistory();

      this.p2p.respondToFileOffer(offer.id, true, offer.from);
      this.showToast('开始接收文件');
      this.currentFileOffer = null;
    } catch (e) {
      console.error('Accept file failed:', e);
      this.showToast('接收文件失败');
    }
  }

  rejectFile() {
    try {
      const modal = document.getElementById('download-modal');
      if (modal) modal.style.display = 'none';
      if (this.currentFileOffer) {
        this.p2p.respondToFileOffer(this.currentFileOffer.id, false, this.currentFileOffer.from);
      }
      this.currentFileOffer = null;
    } catch (e) {
      console.error('Reject file failed:', e);
    }
  }

  showInput(title, defaultVal, callback) {
    const titleEl = document.getElementById('input-title');
    const field = document.getElementById('input-field');
    const modal = document.getElementById('input-modal');
    if (titleEl) titleEl.textContent = title;
    if (field) field.value = defaultVal;
    if (modal) modal.style.display = 'flex';
    this.inputCallback = callback;
    if (field) field.focus();
  }

  showConfirm(text, callback) {
    const textEl = document.getElementById('confirm-text');
    const modal = document.getElementById('confirm-modal');
    const confirmBtn = document.getElementById('btn-confirm');
    if (textEl) textEl.textContent = text;
    if (modal) modal.style.display = 'flex';
    if (confirmBtn) {
      confirmBtn.onclick = () => {
        if (modal) modal.style.display = 'none';
        try { callback(); } catch (e) { console.error('Confirm callback failed:', e); }
      };
    }
  }

  showToast(text) {
    const toast = document.getElementById('toast');
    const toastText = document.getElementById('toast-text');
    if (!toast || !toastText) return;
    toastText.textContent = text;
    toast.style.display = 'block';
    clearTimeout(this._toastTimer);
    this._toastTimer = setTimeout(() => { toast.style.display = 'none'; }, 2000);
  }

  notify(title, body) {
    try {
      if ('Notification' in window && Notification.permission === 'granted') {
        new Notification(title, { body });
      }
    } catch (e) {
      console.error('Notification failed:', e);
    }
  }

  clearUnread(peerId) {
    this.unreadCount.set(peerId, 0);
    this.updateUnreadBadge();
    this.persistUnread();
    this.renderChatList();
  }

  incrementUnread(peerId) {
    const count = (this.unreadCount.get(peerId) || 0) + 1;
    this.unreadCount.set(peerId, count);
    this.updateUnreadBadge();
    this.persistUnread();
  }

  updateUnreadBadge() {
    let total = 0;
    for (const count of this.unreadCount.values()) total += count;
    const badge = document.getElementById('unread-badge');
    if (!badge) return;
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
    try {
      const binary = atob(base64);
      const len = binary.length;
      const bytes = new Uint8Array(len);
      for (let i = 0; i < len; i++) bytes[i] = binary.charCodeAt(i);
      return new Blob([bytes], { type: mimeType });
    } catch (e) {
      console.error('Base64 to blob failed:', e);
      return new Blob([], { type: mimeType });
    }
  }
}

document.addEventListener('DOMContentLoaded', () => {
  window.ppx = new PPXApp();
});