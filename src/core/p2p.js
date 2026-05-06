import { generateUUID } from '../utils/uuid.js';

class EventEmitter {
  constructor() {
    this.events = {};
  }

  on(event, listener) {
    if (!this.events[event]) {
      this.events[event] = [];
    }
    this.events[event].push(listener);
    return this;
  }

  once(event, listener) {
    const wrapper = (...args) => {
      listener(...args);
      this.off(event, wrapper);
    };
    return this.on(event, wrapper);
  }

  off(event, listener) {
    if (!this.events[event]) return this;
    this.events[event] = this.events[event].filter(l => l !== listener);
    return this;
  }

  emit(event, ...args) {
    if (!this.events[event]) return false;
    this.events[event].forEach(listener => listener(...args));
    return true;
  }

  removeAllListeners(event) {
    if (event) {
      delete this.events[event];
    } else {
      this.events = {};
    }
    return this;
  }
}

export class P2PChat extends EventEmitter {
  constructor() {
    super();
    this.peerId = this.generateId();
    this.peers = new Map();
    this.localInfo = null;
    this.broadcastSocket = null;
    this.messageChannel = null;
    this.isWebRTCSupported = typeof RTCPeerConnection !== 'undefined';
    this.heartbeatInterval = null;
    this.peerTimeout = 10000;
    this.BROADCAST_CHANNEL = 'ipmsg-torrent-discovery';
    this.MESSAGE_CHANNEL = 'ipmsg-torrent-messages';
  }

  generateId() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
      const r = Math.random() * 16 | 0;
      const v = c === 'x' ? r : (r & 0x3 | 0x8);
      return v.toString(16);
    });
  }

  async init(username) {
    this.localInfo = {
      id: this.peerId,
      username: username || `用户_${this.generateId().substring(0, 4)}`,
      hostname: this.getHostname(),
      platform: this.getPlatform(),
      addresses: await this.getLocalIPs(),
      timestamp: Date.now()
    };

    this.startBroadcast();
    this.startMessageServer();
    this.startHeartbeat();
    this.cleanupStalePeers();

    this.emit('ready', this.localInfo);
    this.emit('status', 'online');

    return this.localInfo;
  }

  getHostname() {
    if (typeof window !== 'undefined') {
      const ua = window.navigator.userAgent;
      if (/mobile|android|iphone/i.test(ua)) {
        return 'Mobile';
      } else if (/tablet|ipad/i.test(ua)) {
        return 'Tablet';
      }
      return 'Desktop';
    }
    return 'Unknown';
  }

  getPlatform() {
    if (typeof navigator !== 'undefined') {
      return navigator.platform || 'Unknown';
    }
    if (typeof process !== 'undefined' && process.platform) {
      return process.platform;
    }
    return 'Unknown';
  }

  async getLocalIPs() {
    const addresses = [];

    if (typeof window !== 'undefined' && window.RTCPeerConnection) {
      try {
        const pc = new RTCPeerConnection({ iceServers: [] });
        pc.createDataChannel('');

        await new Promise((resolve) => {
          const timeoutId = setTimeout(resolve, 1000);

          pc.onicecandidate = (e) => {
            if (e.candidate) {
              const match = /([0-9]{1,3}(\.[0-9]{1,3}){3}|[a-f0-9]{1,4}(:[a-f0-9]{1,4}){7})/.exec(e.candidate.candidate);
              if (match) {
                addresses.push(match[1]);
              }
            }
          };

          pc.createOffer()
            .then(offer => pc.setLocalDescription(offer))
            .catch(() => clearTimeout(timeoutId));
        });

        pc.close();
      } catch (e) {
        console.warn('Failed to get local IP:', e);
      }
    }

    return addresses.length > 0 ? addresses : ['127.0.0.1'];
  }

  startBroadcast() {
    if (typeof BroadcastChannel === 'undefined') {
      console.warn('BroadcastChannel not supported');
      return;
    }

    try {
      this.broadcastSocket = new BroadcastChannel(this.BROADCAST_CHANNEL);

      this.broadcastSocket.onmessage = (event) => {
        const data = event.data;
        if (data.type === 'presence' || data.type === 'presence-response') {
          if (data.id !== this.peerId) {
            this.handlePresence(data);
          }
        } else if (data.type === 'heartbeat') {
          if (data.id !== this.peerId) {
            this.handleHeartbeat(data);
          }
        }
      };

      this.broadcastSocket.onerror = (error) => {
        console.error('BroadcastChannel error:', error);
      };

    } catch (e) {
      console.error('Failed to start broadcast:', e);
    }
  }

  startMessageServer() {
    if (typeof BroadcastChannel === 'undefined') {
      console.warn('BroadcastChannel not supported for messages');
      return;
    }

    try {
      this.messageChannel = new BroadcastChannel(this.MESSAGE_CHANNEL);

      this.messageChannel.onmessage = (event) => {
        const data = event.data;
        if (data.to === this.peerId || data.to === 'broadcast') {
          this.handleMessage(data);
        }
      };

      this.messageChannel.onerror = (error) => {
        console.error('Message channel error:', error);
      };

    } catch (e) {
      console.error('Failed to start message server:', e);
    }
  }

  startHeartbeat() {
    this.heartbeatInterval = setInterval(() => {
      this.broadcastPresence();
    }, 3000);
  }

  cleanupStalePeers() {
    setInterval(() => {
      const now = Date.now();
      let updated = false;

      for (const [id, peer] of this.peers.entries()) {
        if (now - peer.timestamp > this.peerTimeout) {
          this.peers.delete(id);
          updated = true;
          this.emit('peer-left', peer);
        }
      }

      if (updated) {
        this.emit('peers-updated', Array.from(this.peers.values()));
      }
    }, 5000);
  }

  broadcastPresence() {
    if (this.broadcastSocket && this.localInfo) {
      const presence = {
        type: 'presence',
        ...this.localInfo,
        timestamp: Date.now()
      };
      this.broadcastSocket.postMessage(presence);
    }
  }

  handlePresence(peerInfo) {
    const existingPeer = this.peers.get(peerInfo.id);

    if (!existingPeer) {
      this.peers.set(peerInfo.id, peerInfo);
      this.emit('peer-joined', peerInfo);

      if (this.broadcastSocket) {
        this.broadcastSocket.postMessage({
          type: 'presence-response',
          ...this.localInfo,
          timestamp: Date.now()
        });
      }
    } else {
      Object.assign(existingPeer, peerInfo);
      existingPeer.timestamp = Date.now();
    }

    this.emit('peers-updated', Array.from(this.peers.values()));
  }

  handleHeartbeat(peerInfo) {
    if (this.peers.has(peerInfo.id)) {
      const peer = this.peers.get(peerInfo.id);
      peer.timestamp = Date.now();
    }
  }

  handleMessage(data) {
    switch (data.type) {
      case 'chat':
        this.emit('message', data);
        break;
      case 'file-offer':
        this.emit('file-offer', data);
        break;
      case 'file-accept':
        this.emit('file-accept', data);
        break;
      case 'file-reject':
        this.emit('file-reject', data);
        break;
      case 'typing':
        this.emit('typing', data);
        break;
      case 'read-receipt':
        this.emit('read-receipt', data);
        break;
      default:
        console.warn('Unknown message type:', data.type);
    }
  }

  sendMessage(toPeerId, content, type = 'text') {
    if (!toPeerId) {
      console.warn('No recipient specified');
      return false;
    }

    const message = {
      type: 'chat',
      id: generateUUID(),
      from: this.peerId,
      fromName: this.localInfo?.username,
      to: toPeerId,
      contentType: type,
      content: content,
      timestamp: Date.now()
    };

    this.sendBroadcast(message);
    this.emit('message-sent', message);
    return true;
  }

  sendTyping(toPeerId) {
    if (!toPeerId) return;

    this.sendBroadcast({
      type: 'typing',
      from: this.peerId,
      to: toPeerId,
      timestamp: Date.now()
    });
  }

  sendReadReceipt(toPeerId, messageId) {
    if (!toPeerId || !messageId) return;

    this.sendBroadcast({
      type: 'read-receipt',
      from: this.peerId,
      to: toPeerId,
      messageId: messageId,
      timestamp: Date.now()
    });
  }

  sendBroadcast(data) {
    if (this.messageChannel) {
      try {
        this.messageChannel.postMessage(data);
      } catch (e) {
        console.error('Failed to send broadcast:', e);
      }
    }
  }

  async shareFile(fileBlob, fileName, fileSize) {
    return new Promise((resolve, reject) => {
      if (!fileBlob) {
        reject(new Error('No file provided'));
        return;
      }

      const fileId = generateUUID();
      const reader = new FileReader();

      reader.onload = (e) => {
        try {
          const arrayBuffer = e.target.result;
          const base64 = this.arrayBufferToBase64(arrayBuffer);

          resolve({
            id: fileId,
            name: fileName || 'unknown',
            size: fileSize || 0,
            data: base64,
            type: 'base64',
            mimeType: fileBlob.type || 'application/octet-stream'
          });
        } catch (error) {
          reject(error);
        }
      };

      reader.onerror = () => {
        reject(new Error('Failed to read file'));
      };

      reader.readAsArrayBuffer(fileBlob);
    });
  }

  arrayBufferToBase64(buffer) {
    const bytes = new Uint8Array(buffer);
    let binary = '';
    const len = bytes.byteLength;
    for (let i = 0; i < len; i++) {
      binary += String.fromCharCode(bytes[i]);
    }
    return btoa(binary);
  }

  base64ToArrayBuffer(base64) {
    const binary = atob(base64);
    const len = binary.length;
    const bytes = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
      bytes[i] = binary.charCodeAt(i);
    }
    return bytes.buffer;
  }

  sendFileOffer(toPeerId, fileInfo) {
    if (!toPeerId || !fileInfo) {
      console.warn('Invalid file offer parameters');
      return false;
    }

    const message = {
      type: 'file-offer',
      id: generateUUID(),
      from: this.peerId,
      fromName: this.localInfo?.username,
      to: toPeerId,
      file: fileInfo,
      timestamp: Date.now()
    };

    this.sendBroadcast(message);
    return true;
  }

  respondToFileOffer(offerId, accept, fromPeerId) {
    if (!offerId || !fromPeerId) {
      console.warn('Invalid file offer response parameters');
      return false;
    }

    const message = {
      type: accept ? 'file-accept' : 'file-reject',
      id: offerId,
      from: this.peerId,
      to: fromPeerId,
      timestamp: Date.now()
    };

    this.sendBroadcast(message);
    return true;
  }

  getPeer(id) {
    return this.peers.get(id);
  }

  getAllPeers() {
    return Array.from(this.peers.values());
  }

  getPeerCount() {
    return this.peers.size;
  }

  isOnline() {
    return this.localInfo !== null;
  }

  getLocalInfo() {
    return this.localInfo;
  }

  getMyId() {
    return this.peerId;
  }

  updateUsername(newUsername) {
    if (this.localInfo) {
      this.localInfo.username = newUsername;
      this.broadcastPresence();
    }
  }

  cleanup() {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }

    if (this.broadcastSocket) {
      this.broadcastSocket.close();
      this.broadcastSocket = null;
    }

    if (this.messageChannel) {
      this.messageChannel.close();
      this.messageChannel = null;
    }

    this.peers.clear();
    this.removeAllListeners();

    this.emit('status', 'offline');
  }
}

export default P2PChat;
