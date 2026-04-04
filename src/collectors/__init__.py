from .web_search import BingSearchCollector
from .rss_monitor import RSSCollector
from .url_watcher import URLWatcherCollector
from .email_parser import EmailAlertCollector
from .folder_watcher import FolderWatcherCollector

__all__ = [
    "BingSearchCollector",
    "RSSCollector",
    "URLWatcherCollector",
    "EmailAlertCollector",
    "FolderWatcherCollector",
]
