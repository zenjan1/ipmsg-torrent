const WebTorrent = require('webtorrent');
const { v4: uuidv4 } = require('uuid');
const EventEmitter = require('events');
const dgram = require('dgram');
const os = require('os');
const crypto = require('crypto');

class P2PChat extends EventEmitter {
  constructor() {
    super();
    this.client = null;
    this.peerId = uuidv4();
    this.peers = new Map(); // peerId -> { info, dataChannel }
    this.localInfo = null;
    this.BROADCAST_PORT = 2425;
    this.MESSAGE_PORT = 2426;
    this.broadcastSocket = null;
    this.messageSocket = null;
    this.torrentClient = null;
    this.infoHash = 'ipmsg-torrent-chat-2024';
  }

  async init(username) {
    this.localInfo = {
      id: this.peerId,
      username: username,
      hostname: os.hostname(),
      addresses: this.getLocalIPs(),
      timestamp: Date.now()
    };

    this.startBroadcast();
    this.startMessageServer();
    this.initTorrentClient();
    
    this.emit('ready', this.localInfo);
    this.emit('status', 'online');
  }

  getLocalIPs() {
    const interfaces = os.networkInterfaces();
    const addresses = [];
    
    for (const name of Object.keys(interfaces)) {
      for (const iface of interfaces[name]) {
        if (iface.family === 'IPv4' && !iface.internal) {
          addresses.push(iface.address);
        }
      }
    }
    return addresses;
  }

  startBroadcast() {
    this.broadcastSocket = dgram.createSocket('udp4');
    
    this.broadcastSocket.bind(this.BROADCAST_PORT, () => {
      this.broadcastSocket.setBroadcast(true);
    });

    this.broadcastSocket.on('message', (msg, rinfo) => {
      try {
        const data = JSON.parse(msg.toString());
        if (data.type === 'presence' && data.id !== this.peerId) {
          this.handlePresence(data, rinfo.address);
        } else if (data.type === 'presence-response' && data.id !== this.peerId) {
          this.handlePresence(data, rinfo.address);
        }
      } catch (e) {
        console.error('Broadcast message parse error:', e);
      }
    });

    setInterval(() => {
      this.broadcastPresence();
    }, 3000);

    this.broadcastPresence();
  }

  broadcastPresence() {
    const message = Buffer.from(JSON.stringify({
      type: 'presence',
      ...this.localInfo
    }));

    this.localInfo.addresses.forEach(addr => {
      const network = addr.split('.').slice(0, 3).join('.') + '.255';
      this.broadcastSocket.send(message, this.BROADCAST_PORT, network);
    });
  }

  handlePresence(peerInfo, address) {
    if (!this.peers.has(peerInfo.id)) {
      peerInfo.address = address;
      this.peers.set(peerInfo.id, peerInfo);
      this.emit('peer-joined', peerInfo);
      
      const response = Buffer.from(JSON.stringify({
        type: 'presence-response',
        ...this.localInfo
      }));
      this.broadcastSocket.send(response, this.BROADCAST_PORT, address);
    } else {
      const existing = this.peers.get(peerInfo.id);
      existing.timestamp = Date.now();
      existing.address = address;
    }
    this.emit('peers-updated', Array.from(this.peers.values()));
  }

  startMessageServer() {
    this.messageSocket = dgram.createSocket('udp4');
    
    this.messageSocket.bind(this.MESSAGE_PORT, '0.0.0.0');
    
    this.messageSocket.on('message', (msg, rinfo) => {
      try {
        const data = JSON.parse(msg.toString());
        this.handleMessage(data, rinfo.address);
      } catch (e) {
        console.error('Message parse error:', e);
      }
    });
  }

  handleMessage(data, address) {
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
    }
  }

  sendMessage(toPeerId, content, type = 'text') {
    const peer = this.peers.get(toPeerId);
    if (!peer) return false;

    const message = {
      type: 'chat',
      id: uuidv4(),
      from: this.peerId,
      to: toPeerId,
      contentType: type,
      content: content,
      timestamp: Date.now()
    };

    this.sendUDP(message, peer.address);
    this.emit('message-sent', message);
    return true;
  }

  sendUDP(data, address) {
    const message = Buffer.from(JSON.stringify(data));
    this.messageSocket.send(message, this.MESSAGE_PORT, address);
  }

  initTorrentClient() {
    this.torrentClient = new WebTorrent();
  }

  async shareFile(filePath) {
    return new Promise((resolve, reject) => {
      this.torrentClient.seed(filePath, (torrent) => {
        resolve({
          magnetURI: torrent.magnetURI,
          infoHash: torrent.infoHash,
          files: torrent.files
        });
      });
    });
  }

  async downloadFile(magnetURI, savePath) {
    return new Promise((resolve, reject) => {
      this.torrentClient.add(magnetURI, { path: savePath }, (torrent) => {
        torrent.on('done', () => {
          resolve(torrent.files);
        });
        torrent.on('download', (bytes) => {
          this.emit('download-progress', {
            progress: torrent.progress,
            downloaded: torrent.downloaded,
            total: torrent.length
          });
        });
      });
    });
  }

  sendFileOffer(toPeerId, fileInfo) {
    const peer = this.peers.get(toPeerId);
    if (!peer) return false;

    const message = {
      type: 'file-offer',
      id: uuidv4(),
      from: this.peerId,
      to: toPeerId,
      file: fileInfo,
      timestamp: Date.now()
    };

    this.sendUDP(message, peer.address);
    return true;
  }

  respondToFileOffer(offerId, accept, fromPeerId) {
    const peer = this.peers.get(fromPeerId);
    if (!peer) return false;

    const message = {
      type: accept ? 'file-accept' : 'file-reject',
      id: offerId,
      from: this.peerId,
      to: fromPeerId,
      timestamp: Date.now()
    };

    this.sendUDP(message, peer.address);
    return true;
  }

  cleanup() {
    if (this.broadcastSocket) {
      this.broadcastSocket.close();
    }
    if (this.messageSocket) {
      this.messageSocket.close();
    }
    if (this.torrentClient) {
      this.torrentClient.destroy();
    }
  }
}

module.exports = P2PChat;
