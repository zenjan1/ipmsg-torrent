const { invoke } = window.__TAURI__.core;
const { listen } = window.__TAURI__.event;

let currentTarget = null; // peer_id or #channel
let currentIsChannel = false;
let myPeerId = '';

// --- Init ---
document.getElementById('username-input').addEventListener('keydown', e => {
    if (e.key === 'Enter') connect();
});
document.getElementById('msg-input').addEventListener('keydown', e => {
    if (e.key === 'Enter') sendMessage();
});
document.getElementById('channel-input').addEventListener('keydown', e => {
    if (e.key === 'Enter') joinChannel();
});

async function connect() {
    const username = document.getElementById('username-input').value.trim();
    if (!username) return;

    const btn = document.getElementById('connect-btn');
    btn.textContent = 'Connecting...';
    btn.disabled = true;

    try {
        const peerId = await invoke('p2p_start', {
            args: { username, bootstrap_nodes: [], data_dir: '' }
        });
        myPeerId = peerId;
        document.getElementById('my-id').textContent = peerId.slice(0, 20) + '...';
        document.getElementById('login-screen').classList.add('hidden');
        document.getElementById('chat-screen').classList.remove('hidden');

        // Auto-join general channel
        await invoke('p2p_join_channel', { args: { name: 'general' } });
        selectChannel('general');

        // Listen for events
        await listen('p2p-event', handleEvent);
    } catch (e) {
        btn.textContent = 'Error: ' + e;
        btn.disabled = false;
    }
}

// --- Event handler ---
function handleEvent(event) {
    const evt = event.payload;
    switch (evt.type) {
        case 'PeerJoined':
            addPeer(evt.peer_id, evt.username);
            addSystemMsg(`${evt.username} joined`);
            break;
        case 'PeerLeft':
            removePeer(evt.peer_id);
            addSystemMsg(`Peer left`);
            break;
        case 'MessageReceived': {
            const isDm = !currentIsChannel;
            const fromPeer = evt.from;
            if (currentIsChannel || fromPeer === currentTarget) {
                addMessage(evt.from, evt.content || '', false, evt.timestamp);
            }
            break;
        }
        case 'MessageSent':
            if (!currentIsChannel) {
                addMessage(myPeerId.slice(0, 8), evt.content || '', true, evt.timestamp);
            }
            break;
        case 'Typing':
            setStatus(`${evt.from} is typing...`);
            break;
        case 'Status':
            setStatus(evt.Status || evt.message || '');
            break;
        case 'Ready':
            setStatus('Connected');
            break;
        case 'Error':
            setStatus('Error: ' + (evt.Error || evt.message || ''));
            break;
        default:
            if (evt.Status) setStatus(evt.Status);
            break;
    }
}

// --- Messaging ---
async function sendMessage() {
    const input = document.getElementById('msg-input');
    const text = input.value.trim();
    if (!text || !currentTarget) return;

    input.value = '';
    try {
        if (currentIsChannel) {
            await invoke('p2p_send_channel', {
                args: { channel: currentTarget, content: text }
            });
        } else {
            await invoke('p2p_send', {
                args: { to: currentTarget, content: text }
            });
        }
        addMessage(myPeerId.slice(0, 8), text, true, new Date().toISOString());
    } catch (e) {
        addSystemMsg('Send failed: ' + e);
    }
}

// --- Channels ---
async function joinChannel() {
    const input = document.getElementById('channel-input');
    const name = input.value.trim();
    if (!name) return;
    input.value = '';

    try {
        await invoke('p2p_join_channel', { args: { name } });
        addChannelItem(name);
        selectChannel(name);
    } catch (e) {
        addSystemMsg('Join failed: ' + e);
    }
}

function selectChannel(name) {
    currentTarget = name;
    currentIsChannel = true;
    document.getElementById('current-channel').textContent = '#' + name;
    document.querySelectorAll('.channel-item').forEach(el => {
        el.classList.toggle('active', el.dataset.name === name);
    });
    document.querySelectorAll('.peer-item').forEach(el => el.classList.remove('active'));
    clearMessages();
    loadHistory();
}

function selectPeer(peerId, username) {
    currentTarget = peerId;
    currentIsChannel = false;
    document.getElementById('current-channel').textContent = '@' + username;
    document.querySelectorAll('.peer-item').forEach(el => {
        el.classList.toggle('active', el.dataset.id === peerId);
    });
    document.querySelectorAll('.channel-item').forEach(el => el.classList.remove('active'));
    clearMessages();
    loadHistory();
}

// --- Peers ---
const peers = new Map();

function addPeer(peerId, username) {
    if (peers.has(peerId)) return;
    peers.set(peerId, username);
    renderPeers();
}

function removePeer(peerId) {
    peers.delete(peerId);
    renderPeers();
}

function renderPeers() {
    const list = document.getElementById('peer-list');
    document.getElementById('peer-count').textContent = peers.size;
    list.innerHTML = '';
    for (const [id, name] of peers) {
        const div = document.createElement('div');
        div.className = 'peer-item';
        div.dataset.id = id;
        div.innerHTML = `<span class="peer-dot"></span>${name}`;
        div.onclick = () => selectPeer(id, name);
        list.appendChild(div);
    }
}

function addChannelItem(name) {
    const list = document.getElementById('channel-list');
    if (document.querySelector(`.channel-item[data-name="${name}"]`)) return;
    const div = document.createElement('div');
    div.className = 'channel-item';
    div.dataset.name = name;
    div.textContent = '#' + name;
    div.onclick = () => selectChannel(name);
    list.appendChild(div);
}

// --- Messages ---
function addMessage(author, content, isOwn, timestamp) {
    const container = document.getElementById('messages');
    const div = document.createElement('div');
    div.className = 'msg ' + (isOwn ? 'own' : 'other');

    const time = timestamp ? new Date(timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) : '';
    div.innerHTML = `${isOwn ? '' : `<div class="msg-author">${esc(author)}</div>`}
        <div>${esc(content)}</div>
        <div class="msg-time">${time}</div>`;
    container.appendChild(div);
    container.scrollTop = container.scrollHeight;
}

function addSystemMsg(text) {
    const container = document.getElementById('messages');
    const div = document.createElement('div');
    div.className = 'msg system';
    div.textContent = text;
    container.appendChild(div);
    container.scrollTop = container.scrollHeight;
}

function clearMessages() {
    document.getElementById('messages').innerHTML = '';
}

async function loadHistory() {
    try {
        const limit = 100;
        let messages;
        if (currentIsChannel) {
            // Channel history not directly supported, skip for now
            return;
        } else {
            messages = await invoke('p2p_get_history', {
                args: { peer_id: currentTarget, limit }
            });
        }
        if (typeof messages === 'string') messages = JSON.parse(messages);
        if (Array.isArray(messages)) {
            for (const msg of messages) {
                const isOwn = msg.from === myPeerId;
                const content = msg.kind?.Text || msg.kind?.text || '';
                addMessage(
                    isOwn ? myPeerId.slice(0, 8) : (msg.from || '').slice(0, 8),
                    content, isOwn, msg.timestamp
                );
            }
        }
    } catch (_) {}
}

// --- Helpers ---
function setStatus(text) {
    document.getElementById('status-text').textContent = text;
}

function esc(s) {
    const el = document.createElement('span');
    el.textContent = s;
    return el.innerHTML;
}
