const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('electronAPI', {
  selectFile: (options) => ipcRenderer.invoke('select-file', options),
  selectDirectory: () => ipcRenderer.invoke('select-directory'),
  saveFile: (data) => ipcRenderer.invoke('save-file', data),
  saveFileBuffer: (data) => ipcRenderer.invoke('save-file-buffer', data),
  getAppPath: () => ipcRenderer.invoke('get-app-path'),
  getPlatform: () => ipcRenderer.invoke('get-platform'),
  showNotification: (data) => ipcRenderer.invoke('show-notification', data),

  onMenuOpenFile: (callback) => {
    ipcRenderer.on('menu-open-file', () => callback());
  },
  onMenuSaveHistory: (callback) => {
    ipcRenderer.on('menu-save-history', () => callback());
  },

  removeAllListeners: (channel) => {
    ipcRenderer.removeAllListeners(channel);
  }
});

window.addEventListener('DOMContentLoaded', () => {
  console.log('Preload script loaded');
});
