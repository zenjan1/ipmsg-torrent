# Android 镜像源配置说明

## 📌 已配置的镜像源

项目已配置 **阿里云镜像源**（抖音旗下），包含以下仓库：

| 仓库类型 | URL | 用途 |
|---------|-----|------|
| Google 仓库 | https://maven.aliyun.com/repository/google | Android 官方依赖 |
| Central 仓库 | https://maven.aliyun.com/repository/central | Maven 中央库 |
| Public 仓库 | https://maven.aliyun.com/repository/public | 公共库 |
| Gradle 插件 | https://maven.aliyun.com/repository/gradle-plugin | Gradle 插件 |

---

## 📝 配置文件位置

已修改的配置文件：

1. **[android/build.gradle](android/build.gradle)** - 根项目配置
2. **[android/settings.gradle](android/settings.gradle)** - 插件和依赖配置
3. **[android/gradle.properties](android/gradle.properties)** - 网络优化配置
4. **[android/init.gradle](android/init.gradle)** - 全局初始化脚本

---

## 🚀 构建步骤

### 方法一：使用自动化脚本（推荐）

```bash
# 进入项目根目录
cd /workspace

# 给脚本执行权限
chmod +x build-android.sh

# 运行构建脚本
./build-android.sh
```

### 方法二：手动构建

```bash
cd /workspace/android

# 1. 清理之前的构建缓存
rm -rf .gradle build app/build

# 2. 返回根目录，构建 Web 版本
cd ..
npm run build:web

# 3. 同步 Web 资源到 Android
npx cap sync android

# 4. 进入 Android 目录构建
cd android
./gradlew assembleDebug --no-daemon
```

---

## 🎯 镜像源备选方案

如果阿里云镜像源仍有问题，可以尝试以下替代方案：

### 1. 腾讯云镜像
```gradle
maven { url 'https://mirrors.cloud.tencent.com/nexus/repository/maven-public/' }
maven { url 'https://mirrors.cloud.tencent.com/nexus/repository/google/' }
```

### 2. 华为云镜像
```gradle
maven { url 'https://mirrors.huaweicloud.com/repository/maven/' }
```

### 3. 南京大学镜像
```gradle
maven { url 'https://maven.nju.edu.cn/repository/maven-public/' }
```

---

## 🔧 网络问题排查

### 问题 1：连接超时
检查 `gradle.properties` 中的超时配置：
```properties
systemProp.http.connectionTimeout=120000
systemProp.http.socketTimeout=120000
```

### 问题 2：DNS 解析问题
可以尝试配置本地 hosts 文件，或使用 IP 直连。

### 问题 3：Gradle 版本
当前使用的 Gradle 版本是 8.2.0，如有兼容性问题可以调整版本。

---

## 📊 输出文件

构建成功后，APK 位置：
```
android/app/build/outputs/apk/debug/app-debug.apk
```

---

## 💡 注意事项

1. **首次构建**会下载依赖，可能需要较长时间
2. 确保有足够的磁盘空间（至少 1GB）
3. 如果构建失败，可以先尝试 `./gradlew clean` 清理缓存
4. 使用 `--no-daemon` 参数避免后台进程问题

---

**最后更新**: 2026-05-07
