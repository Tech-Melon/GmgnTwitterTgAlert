from playwright.async_api import BrowserContext, Page, Playwright
from loguru import logger

from . import config


class BrowserManager:
    def __init__(self):
        self.context: BrowserContext | None = None
        self.page: Page | None = None

    async def launch(self, playwright: Playwright) -> Page:
        logger.info(f"正在启动浏览器，使用持久化数据目录: {config.USER_DATA_DIR}")
        self.context = await playwright.chromium.launch_persistent_context(
            user_data_dir=config.USER_DATA_DIR,
            headless=False,
            proxy={"server": config.PROXY_SERVER},
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--window-size=1920,1080",
                "--start-maximized",
            ],
        )
        self.page = self.context.pages[0] if self.context.pages else await self.context.new_page()
        return self.page

    async def run_first_login_if_needed(self):
        if not config.FIRST_RUN_LOGIN:
            return

        logger.info("检测到开启了首次运行登录模式，正在访问授权登录网页...")
        await self.page.goto(config.AUTH_URL, wait_until="networkidle")
        logger.info("授权网页加载完成，正在等待 8 秒钟让网站将凭证写入本地缓存文件...")
        await self.page.wait_for_timeout(8000)
        logger.success("网站缓存吸录完毕！下一次启动可将 FIRST_RUN_LOGIN 改回 False。")

    async def goto_monitor_page(self):
        logger.info(f"正在跳转监控目标网站: {config.MONITOR_URL}")
        await self.page.goto(config.MONITOR_URL, wait_until="networkidle")
        await self.page.wait_for_timeout(5000)

    async def handle_popups(self):
        logger.info("正在尝试处理可能存在的更新提示弹窗...")
        for _ in range(5):
            try:
                next_btn = self.page.locator("button:has-text('Next'), button:has-text('Complete'), button:has-text('下一步'), button:has-text('完成')").first
                if await next_btn.is_visible(timeout=1000):
                    logger.info("发现更新提示继续按钮，正在点击关闭...")
                    await next_btn.click()
                    await self.page.wait_for_timeout(500)
                else:
                    break
            except Exception:
                break

        try:
            await self.page.keyboard.press("Escape")
            await self.page.mouse.click(10, 10)
            await self.page.wait_for_timeout(1000)
        except Exception:
            pass

    async def switch_to_mine_tab(self):
        try:
            my_tab = self.page.locator("xpath=//*[text()='我的' or text()='Mine' or text()='关注' or text()='Following']").first
            if await my_tab.is_visible(timeout=2000):
                logger.info("找到【我的/Mine/Following】标签，正在切换...")
                await my_tab.click()
                await self.page.wait_for_timeout(2000)
            else:
                logger.warning("未能通过精确文字找到【我的/Mine/Following】标签元素，尝试通过相关类名寻找...")
                backup_tab = self.page.locator("span:has-text('我的'), span:has-text('Mine'), span:has-text('关注'), span:has-text('Following')").first
                if await backup_tab.is_visible():
                    await backup_tab.click()
                    await self.page.wait_for_timeout(2000)
                else:
                    raise RuntimeError("无法定位到目标标签页！可能是 UI 更改或登录状态（Cookie）已失效。")
        except Exception as e:
            logger.error(f"切换标签页时出错: {e}")
            raise

    async def save_screenshot(self):
        await self.page.screenshot(path=config.SCREENSHOT_PATH)
        logger.info(f"界面已准备完毕，运行截图已保存: {config.SCREENSHOT_PATH}")

    async def recover_after_timeout(self):
        await self.page.reload(wait_until="domcontentloaded")
        logger.success("网页刷新指令下发完成，看门狗周期重置。")
        await self.page.wait_for_timeout(5000)
        await self.switch_to_mine_tab()

    async def close(self):
        if self.context:
            await self.context.close()
