# Android Release 构建说明

## 已完成工作

1. ✅ 版本号已更新到 1.2.0
2. ✅ Web 资源已成功构建并同步到 Android 项目
3. ✅ 阿里云镜像源已配置 (settings.gradle, build.gradle, init.gradle)
4. ✅ 所有必要的依赖声明已就绪

## 构建步骤

在有网络连接的环境下执行：

### 方法1: 使用 npm 脚本 (推荐)

```bash
# 1. 安装依赖
npm install

# 2. 构建并同步 Web 资源
npm run android:build

# 3. 构建 Release APK
npm run android:apk
```

### 方法2: 使用 Gradle 直接构建

```bash
cd android

# 构建 Debug APK (用于测试)
./gradlew assembleDebug

# 构建 Release APK
./gradlew assembleRelease

# 或者使用阿里云镜像源 init 脚本
./gradlew --init-script init.gradle assembleRelease
```

## 输出位置

构建成功后，APK 文件会在以下位置：

- Debug APK: `android/app/build/outputs/apk/debug/app-debug.apk`
- Release APK: `android/app/build/outputs/apk/release/app-release.apk`

## 注意事项

1. 首次构建会下载 Android SDK 依赖，需要较长时间
2. 项目已配置阿里云镜像源，国内访问速度更快
3. 如果网络受限，请确保可以访问以下镜像源：
   - https://maven.aliyun.com/repository/google
   - https://maven.aliyun.com/repository/central
   - https://maven.aliyun.com/repository/gradle-plugin

## 签名配置 (Release 版本)

默认的 Release 构建会尝试使用签名配置。如需配置签名，请：

1. 生成密钥库 (使用 `android/generate-keystore.sh`)
2. 在 `android/app/build.gradle` 中配置 signingConfigs
3. 或者使用 Android Studio 打开项目进行配置

## 当前项目状态

- 版本: 1.2.0
- Web 资源已同步: ✅
- 构建配置已就绪: ✅
- 国内镜像源已配置: ✅
