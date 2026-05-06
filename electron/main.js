import { app, BrowserWindow, ipcMain, dialog, Menu, Tray, nativeImage, shell } from 'electron';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

let mainWindow = null;
let tray = null;
const isDev = process.env.NODE_ENV === 'development' || process.argv.includes('--dev');

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1200,
    height: 800,
    minWidth: 800,
    minHeight: 600,
    title: 'IPMsg Torrent',
    icon: path.join(__dirname, '../build/icon.png'),
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
      enableRemoteModule: false,
      preload: path.join(__dirname, 'preload.js'),
      webSecurity: true,
      allowRunningInsecureContent: false
    },
    show: false,
    backgroundColor: '#667eea'
  });

  mainWindow.once('ready-to-show', () => {
    mainWindow.show();
    if (isDev) {
      mainWindow.webContents.openDevTools();
    }
  });

  if (isDev) {
    mainWindow.loadURL('http://localhost:3000');
    mainWindow.webContents.openDevTools();
  } else {
    mainWindow.loadFile(path.join(__dirname, '../dist/index.html'));
  }

  mainWindow.on('closed', () => {
    mainWindow = null;
  });

  mainWindow.on('close', (event) => {
    if (process.platform === 'darwin') {
      event.preventDefault();
      mainWindow.hide();
    }
  });

  createMenu();
  createTray();
}

function createMenu() {
  const template = [
    {
      label: '文件',
      submenu: [
        {
          label: '打开文件',
          accelerator: 'CmdOrCtrl+O',
          click: () => {
            mainWindow.webContents.send('menu-open-file');
          }
        },
        {
          label: '保存聊天记录',
          accelerator: 'CmdOrCtrl+S',
          click: () => {
            mainWindow.webContents.send('menu-save-history');
          }
        },
        { type: 'separator' },
        {
          label: '退出',
          accelerator: process.platform === 'darwin' ? 'Cmd+Q' : 'Alt+F4',
          click: () => {
            app.quit();
          }
        }
      ]
    },
    {
      label: '编辑',
      submenu: [
        { role: 'undo' },
        { role: 'redo' },
        { type: 'separator' },
        { role: 'cut' },
        { role: 'copy' },
        { role: 'paste' },
        { role: 'selectAll' }
      ]
    },
    {
      label: '视图',
      submenu: [
        { role: 'reload' },
        { role: 'forceReload' },
        { type: 'separator' },
        { role: 'resetZoom' },
        { role: 'zoomIn' },
        { role: 'zoomOut' },
        { type: 'separator' },
        { role: 'togglefullscreen' }
      ]
    },
    {
      label: '帮助',
      submenu: [
        {
          label: '关于',
          click: () => {
            dialog.showMessageBox(mainWindow, {
              type: 'info',
              title: '关于 IPMsg Torrent',
              message: 'IPMsg Torrent v1.0.0',
              detail: '去中心化 P2P 聊天应用\n支持多平台，无需服务器'
            });
          }
        },
        {
          label: '文档',
          click: () => {
            shell.openExternal('https://github.com/ipmsg-torrent');
          }
        }
      ]
    }
  ];

  if (process.platform === 'darwin') {
    template.unshift({
      label: app.getName(),
      submenu: [
        { role: 'about' },
        { type: 'separator' },
        { role: 'services' },
        { type: 'separator' },
        { role: 'hide' },
        { role: 'hideOthers' },
        { role: 'unhide' },
        { type: 'separator' },
        { role: 'quit' }
      ]
    });
  }

  const menu = Menu.buildFromTemplate(template);
  Menu.setApplicationMenu(menu);
}

function createTray() {
  const iconPath = path.join(__dirname, '../build/icon.png');

  try {
    const icon = nativeImage.createFromPath(iconPath);
    tray = new Tray(icon.isEmpty() ? nativeImage.createEmpty() : icon);

    const contextMenu = Menu.buildFromTemplate([
      {
        label: '显示窗口',
        click: () => {
          if (mainWindow) {
            mainWindow.show();
            mainWindow.focus();
          }
        }
      },
      {
        label: '状态: 在线',
        enabled: false
      },
      { type: 'separator' },
      {
        label: '退出',
        click: () => {
          app.quit();
        }
      }
    ]);

    tray.setToolTip('IPMsg Torrent - P2P 聊天');
    tray.setContextMenu(contextMenu);

    tray.on('click', () => {
      if (mainWindow) {
        if (mainWindow.isVisible()) {
          mainWindow.hide();
        } else {
          mainWindow.show();
          mainWindow.focus();
        }
      }
    });
  } catch (e) {
    console.log('Tray icon not found, skipping tray creation');
  }
}

ipcMain.handle('select-file', async (event, options = {}) => {
  const result = await dialog.showOpenDialog(mainWindow, {
    properties: ['openFile', 'multiSelections'],
    filters: options.filters || [
      { name: '所有文件', extensions: ['*'] },
      { name: '图片', extensions: ['jpg', 'png', 'gif', 'bmp'] },
      { name: '文档', extensions: ['pdf', 'doc', 'docx', 'txt'] }
    ]
  });
  return result;
});

ipcMain.handle('select-directory', async () => {
  const result = await dialog.showOpenDialog(mainWindow, {
    properties: ['openDirectory']
  });
  return result;
});

ipcMain.handle('save-file', async (event, { content, defaultPath }) => {
  const result = await dialog.showSaveDialog(mainWindow, {
    defaultPath: defaultPath,
    filters: [
      { name: '所有文件', extensions: ['*'] }
    ]
  });

  if (!result.canceled && result.filePath) {
    const fs = await import('fs/promises');
    await fs.writeFile(result.filePath, content, 'utf-8');
    return { success: true, path: result.filePath };
  }
  return { success: false };
});

ipcMain.handle('save-file-buffer', async (event, { buffer, defaultPath }) => {
  const result = await dialog.showSaveDialog(mainWindow, {
    defaultPath: defaultPath,
    filters: [
      { name: '所有文件', extensions: ['*'] }
    ]
  });

  if (!result.canceled && result.filePath) {
    const fs = await import('fs/promises');
    await fs.writeFile(result.filePath, Buffer.from(buffer));
    return { success: true, path: result.filePath };
  }
  return { success: false };
});

ipcMain.handle('get-app-path', () => {
  return app.getPath('userData');
});

ipcMain.handle('get-platform', () => {
  return process.platform;
});

ipcMain.handle('show-notification', (event, { title, body }) => {
  const { Notification } = require('electron');
  if (Notification.isSupported()) {
    new Notification({ title, body }).show();
  }
});

app.whenReady().then(() => {
  createWindow();

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createWindow();
    } else if (mainWindow) {
      mainWindow.show();
    }
  });
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

app.on('before-quit', () => {
  if (tray) {
    tray.destroy();
  }
});

process.on('uncaughtException', (error) => {
  console.error('Uncaught Exception:', error);
  dialog.showErrorBox('错误', `发生未处理的错误: ${error.message}`);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
});
