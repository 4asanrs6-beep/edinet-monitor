"""EDINET Monitor - GUI (tkinter).

レイアウト:
┌─────────────────────────────────────────────────────┐
│ ツールバー: [カテゴリ▼] [検索] [未読/全て] [更新]    │
├─────────────────┬───────────────────────────────────┤
│ イベント一覧     │ 詳細表示                          │
│ (Treeview)      │ 書類情報 + メモ + アクション       │
├─────────────────┴───────────────────────────────────┤
│ ステータスバー                                       │
└─────────────────────────────────────────────────────┘
"""
import json
import logging
import os
import queue
import sys
import threading
import tkinter as tk
import webbrowser
from datetime import date, datetime, timedelta
from pathlib import Path
from tkinter import font as tkfont
from tkinter import messagebox, ttk
from typing import Optional

from classifier import Classifier
from config import PDF_CACHE_DIR
from models import Document, EventCategory, Priority

logger = logging.getLogger(__name__)

ALL_CATEGORIES = "全て"
CATEGORY_OPTIONS = [ALL_CATEGORIES] + [c.value for c in EventCategory if c != EventCategory.OTHER]

PRIORITY_COLORS = {
    Priority.CRITICAL.value: "#D32F2F",
    Priority.HIGH.value: "#E65100",
    Priority.MEDIUM.value: "#1565C0",
    Priority.LOW.value: "#616161",
}


class EdinetMonitorGUI:
    """メインGUIアプリケーション."""

    def __init__(self, config: dict, storage, monitor, notifier):
        self.config = config
        self.storage = storage
        self.monitor = monitor
        self.notifier = notifier

        self.msg_queue = queue.Queue()
        self.selected_doc: Optional[Document] = None
        self.filter_category = ALL_CATEGORIES
        self.filter_read = "all"
        self.view_date = date.today()
        self.search_text = ""
        self._doc_cache: dict[str, Document] = {}
        self._current_xbrl_data: Optional[dict] = None
        self._screen_notified: set[str] = set()  # 速報通知済みのscreen_iid
        self._screen_docs: dict[str, Document] = {}  # 速報行のDocument（API未検出分を保持）

        self._setup_root()
        self._setup_styles()
        self._create_widgets()
        self._setup_copy_support()
        self._setup_tray()

        self.root.after(100, self._refresh_list)
        self.root.after(200, self._process_queue)

    def _setup_root(self):
        self.root = tk.Tk()
        self.root.title("EDINET Monitor")
        gui_cfg = self.config.get("gui", {})
        w = gui_cfg.get("window_width", 1200)
        h = gui_cfg.get("window_height", 750)
        sw = self.root.winfo_screenwidth()
        sh = self.root.winfo_screenheight()
        x = (sw - w) // 2
        y = (sh - h) // 2
        self.root.geometry(f"{w}x{h}+{x}+{y}")
        self.root.minsize(900, 550)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _setup_styles(self):
        style = ttk.Style()
        style.theme_use("vista" if sys.platform == "win32" else "clam")

        jp_fonts = ["Yu Gothic UI", "Meiryo UI", "Meiryo", "MS Gothic"]
        available = tkfont.families()
        self.jp_font = "TkDefaultFont"
        for f in jp_fonts:
            if f in available:
                self.jp_font = f
                break

        style.configure("Treeview", font=(self.jp_font, 10), rowheight=28)
        style.configure("Treeview.Heading", font=(self.jp_font, 10, "bold"))
        style.configure("TLabel", font=(self.jp_font, 10))
        style.configure("TButton", font=(self.jp_font, 10))
        style.configure("Header.TLabel", font=(self.jp_font, 12, "bold"))
        style.configure("Status.TLabel", font=(self.jp_font, 9))

    def _create_widgets(self):
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=2, pady=2)

        self._create_toolbar(main_frame)

        paned = ttk.PanedWindow(main_frame, orient=tk.HORIZONTAL)
        paned.pack(fill=tk.BOTH, expand=True, pady=(4, 0))

        left_frame = ttk.Frame(paned)
        paned.add(left_frame, weight=4)
        self._create_event_list(left_frame)

        right_frame = ttk.Frame(paned)
        paned.add(right_frame, weight=1)
        self._create_detail_view(right_frame)

        self._create_status_bar(main_frame)

    def _create_toolbar(self, parent):
        toolbar = ttk.Frame(parent)
        toolbar.pack(fill=tk.X, padx=4, pady=4)

        # 日付切替
        ttk.Button(toolbar, text="◀", width=2, command=self._prev_date).pack(side=tk.LEFT)
        self.date_btn_var = tk.StringVar(value=self.view_date.strftime("%m/%d"))
        ttk.Button(toolbar, textvariable=self.date_btn_var, width=6, command=self._today_date).pack(
            side=tk.LEFT, padx=(0, 2)
        )
        ttk.Button(toolbar, text="▶", width=2, command=self._next_date).pack(side=tk.LEFT, padx=(0, 8))

        ttk.Label(toolbar, text="種別:").pack(side=tk.LEFT, padx=(0, 2))
        self.category_var = tk.StringVar(value=ALL_CATEGORIES)
        category_combo = ttk.Combobox(
            toolbar, textvariable=self.category_var,
            values=CATEGORY_OPTIONS, state="readonly", width=16
        )
        category_combo.pack(side=tk.LEFT, padx=(0, 8))
        category_combo.bind("<<ComboboxSelected>>", lambda e: self._on_filter_change())

        ttk.Label(toolbar, text="検索:").pack(side=tk.LEFT, padx=(0, 2))
        self.search_var = tk.StringVar()
        search_entry = ttk.Entry(toolbar, textvariable=self.search_var, width=20)
        search_entry.pack(side=tk.LEFT, padx=(0, 4))
        search_entry.bind("<Return>", lambda e: self._on_filter_change())
        ttk.Button(toolbar, text="検索", width=4, command=self._on_filter_change).pack(
            side=tk.LEFT, padx=(0, 8)
        )

        self.read_filter_var = tk.StringVar(value="all")
        ttk.Radiobutton(
            toolbar, text="全て", variable=self.read_filter_var, value="all",
            command=self._on_filter_change
        ).pack(side=tk.LEFT, padx=2)
        ttk.Radiobutton(
            toolbar, text="未読", variable=self.read_filter_var, value="unread",
            command=self._on_filter_change
        ).pack(side=tk.LEFT, padx=2)

        ttk.Button(toolbar, text="全て既読", width=8, command=self._mark_all_read).pack(
            side=tk.LEFT, padx=(8, 4)
        )

        self.star_filter_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            toolbar, text="★のみ", variable=self.star_filter_var,
            command=self._on_filter_change
        ).pack(side=tk.LEFT, padx=(8, 2))

        # ソート切替
        self.sort_var = tk.StringVar(value="time")
        ttk.Label(toolbar, text="並び:").pack(side=tk.LEFT, padx=(8, 2))
        ttk.Radiobutton(
            toolbar, text="時刻順", variable=self.sort_var, value="time",
            command=self._on_filter_change
        ).pack(side=tk.LEFT, padx=2)
        ttk.Radiobutton(
            toolbar, text="重要度順", variable=self.sort_var, value="priority",
            command=self._on_filter_change
        ).pack(side=tk.LEFT, padx=2)

        ttk.Button(toolbar, text="手動取得", width=8, command=self._manual_poll).pack(
            side=tk.RIGHT, padx=2
        )
        self.monitor_btn_text = tk.StringVar(value="監視停止")
        ttk.Button(
            toolbar, textvariable=self.monitor_btn_text, width=8,
            command=self._toggle_monitor
        ).pack(side=tk.RIGHT, padx=2)

    def _create_event_list(self, parent):
        columns = ("star", "time", "category", "pri", "ticker", "target", "filer")
        self.tree = ttk.Treeview(parent, columns=columns, show="headings", selectmode="browse")

        self.tree.heading("star", text="★", anchor=tk.CENTER)
        self.tree.heading("time", text="時刻", anchor=tk.W)
        self.tree.heading("category", text="種別", anchor=tk.W)
        self.tree.heading("pri", text="優先", anchor=tk.CENTER)
        self.tree.heading("ticker", text="コード", anchor=tk.CENTER)
        self.tree.heading("target", text="対象", anchor=tk.W)
        self.tree.heading("filer", text="提出者", anchor=tk.W)

        self.tree.column("star", width=30, minwidth=25, stretch=False, anchor=tk.CENTER)
        self.tree.column("time", width=50, minwidth=45, stretch=False)
        self.tree.column("category", width=120, minwidth=80, stretch=False)
        self.tree.column("pri", width=40, minwidth=35, stretch=False, anchor=tk.CENTER)
        self.tree.column("ticker", width=50, minwidth=40, stretch=False, anchor=tk.CENTER)
        self.tree.column("target", width=200, minwidth=120)
        self.tree.column("filer", width=200, minwidth=120)

        self.tree.tag_configure("unread", font=(self.jp_font, 10, "bold"))
        self.tree.tag_configure("read", foreground="#888888")
        # 優先度: テキスト色 + 背景色
        self.tree.tag_configure("p1", foreground="#D32F2F", background="#FFF3F3")
        self.tree.tag_configure("p2", foreground="#E65100", background="#FFF8E1")
        self.tree.tag_configure("p3", foreground="#1565C0", background="#F5F5F5")
        self.tree.tag_configure("p4", foreground="#9E9E9E", background="#FAFAFA")
        # 速報行: 黄色背景で目立たせる
        self.tree.tag_configure("screen_flash", background="#FFEB3B", font=(self.jp_font, 10, "bold"))

        scrollbar = ttk.Scrollbar(parent, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)

        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.tree.bind("<<TreeviewSelect>>", self._on_tree_select)
        self.tree.bind("<Double-1>", self._on_tree_double_click)
        self.tree.bind("<Control-c>", self._copy_selected_row)

    def _create_detail_view(self, parent):
        canvas = tk.Canvas(parent, highlightthickness=0)
        scrollbar = ttk.Scrollbar(parent, orient=tk.VERTICAL, command=canvas.yview)
        self.detail_frame = ttk.Frame(canvas)

        self.detail_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        canvas.create_window((0, 0), window=self.detail_frame, anchor=tk.NW)
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        canvas.bind_all("<MouseWheel>", _on_mousewheel)

        self._build_detail_content()

    def _build_detail_content(self):
        f = self.detail_frame
        row = 0

        # ヘッダー
        self.detail_header = ttk.Label(f, text="書類を選択してください", style="Header.TLabel")
        self.detail_header.grid(row=row, column=0, columnspan=2, padx=8, pady=(8, 4), sticky=tk.W)
        row += 1

        ttk.Separator(f, orient=tk.HORIZONTAL).grid(row=row, column=0, columnspan=2, sticky=tk.EW, padx=8, pady=4)
        row += 1

        # アクションボタン（最上部に配置、2行に分けて狭くても全ボタン表示）
        btn_row1 = ttk.Frame(f)
        btn_row1.grid(row=row, column=0, columnspan=2, padx=8, pady=(4, 2), sticky=tk.W)

        self.btn_pdf = ttk.Button(btn_row1, text="PDF表示", command=self._open_pdf, state=tk.DISABLED)
        self.btn_pdf.pack(side=tk.LEFT, padx=(0, 4))

        self.btn_edinet = ttk.Button(btn_row1, text="EDINET原文", command=self._open_edinet, state=tk.DISABLED)
        self.btn_edinet.pack(side=tk.LEFT, padx=(0, 4))

        self.btn_toggle_read = ttk.Button(btn_row1, text="既読にする", command=self._toggle_read, state=tk.DISABLED)
        self.btn_toggle_read.pack(side=tk.LEFT, padx=(0, 4))
        row += 1

        btn_row2 = ttk.Frame(f)
        btn_row2.grid(row=row, column=0, columnspan=2, padx=8, pady=(0, 4), sticky=tk.W)

        self.btn_star = ttk.Button(btn_row2, text="★ お気に入り", command=self._toggle_star, state=tk.DISABLED)
        self.btn_star.pack(side=tk.LEFT, padx=(0, 4))

        self.btn_raw = ttk.Button(btn_row2, text="生データ", command=self._show_raw_json, state=tk.DISABLED)
        self.btn_raw.pack(side=tk.LEFT, padx=(0, 4))
        row += 1

        ttk.Separator(f, orient=tk.HORIZONTAL).grid(row=row, column=0, columnspan=2, sticky=tk.EW, padx=8, pady=8)
        row += 1

        # 重要情報（XBRL）
        ttk.Label(f, text="重要情報:", font=(self.jp_font, 11, "bold")).grid(
            row=row, column=0, columnspan=2, padx=8, pady=(4, 2), sticky=tk.W
        )
        row += 1

        self.xbrl_frame = ttk.Frame(f)
        self.xbrl_frame.grid(row=row, column=0, columnspan=2, padx=8, pady=2, sticky=tk.EW)
        self.xbrl_info_label = ttk.Label(self.xbrl_frame, text="書類を選択すると自動抽出します", foreground="#888")
        self.xbrl_info_label.pack(anchor=tk.W)
        row += 1

        ttk.Separator(f, orient=tk.HORIZONTAL).grid(row=row, column=0, columnspan=2, sticky=tk.EW, padx=8, pady=8)
        row += 1

        # コピー用
        ttk.Label(f, text="コピー用（詳細+重要情報）:", font=(self.jp_font, 10, "bold")).grid(
            row=row, column=0, columnspan=2, padx=8, pady=(2, 2), sticky=tk.W
        )
        row += 1

        self.copy_blob_text = tk.Text(
            f,
            height=8,
            width=64,
            wrap=tk.WORD,
            font=(self.jp_font, 10),
        )
        self.copy_blob_text.grid(row=row, column=0, columnspan=2, padx=8, pady=(0, 4), sticky=tk.EW)
        self.copy_blob_text.configure(state=tk.DISABLED)
        row += 1

        ttk.Separator(f, orient=tk.HORIZONTAL).grid(row=row, column=0, columnspan=2, sticky=tk.EW, padx=8, pady=8)
        row += 1

        # メモ
        ttk.Label(f, text="メモ:", font=(self.jp_font, 10, "bold")).grid(
            row=row, column=0, padx=(8, 4), pady=2, sticky=tk.NE
        )
        self.memo_text = tk.Text(f, height=5, width=50, font=(self.jp_font, 10), wrap=tk.WORD)
        self.memo_text.grid(row=row, column=1, padx=8, pady=2, sticky=tk.EW)
        row += 1

        memo_btn_frame = ttk.Frame(f)
        memo_btn_frame.grid(row=row, column=1, padx=8, pady=2, sticky=tk.W)
        self.btn_save_memo = ttk.Button(memo_btn_frame, text="メモ保存", command=self._save_memo, state=tk.DISABLED)
        self.btn_save_memo.pack(side=tk.LEFT)

        # detail_labels は空 dict（後方互換用）
        self.detail_labels = {}
        self.detail_reason = None

        f.columnconfigure(1, weight=1)

    def _create_status_bar(self, parent):
        status_frame = ttk.Frame(parent, relief=tk.SUNKEN)
        status_frame.pack(fill=tk.X, pady=(4, 0))

        self.status_icon = ttk.Label(status_frame, text="●", foreground="gray", style="Status.TLabel")
        self.status_icon.pack(side=tk.LEFT, padx=(8, 4))

        self.status_label = ttk.Label(status_frame, text="起動中...", style="Status.TLabel")
        self.status_label.pack(side=tk.LEFT, padx=(0, 16))

        self.unread_label = ttk.Label(status_frame, text="未読: 0", style="Status.TLabel")
        self.unread_label.pack(side=tk.RIGHT, padx=8)

        self.codelist_label = ttk.Label(status_frame, text="", style="Status.TLabel")
        self.codelist_label.pack(side=tk.RIGHT, padx=8)

        self.date_label = ttk.Label(
            status_frame,
            text=f"日付: {date.today().strftime('%Y-%m-%d')}",
            style="Status.TLabel",
        )
        self.date_label.pack(side=tk.RIGHT, padx=8)

    def _setup_tray(self):
        try:
            import pystray
            from PIL import Image, ImageDraw

            img = Image.new("RGBA", (64, 64), (0, 0, 0, 0))
            draw = ImageDraw.Draw(img)
            draw.rounded_rectangle([4, 4, 60, 60], radius=8, fill=(33, 150, 83))
            draw.text((18, 14), "E", fill="white")

            def on_show(icon, item):
                self.root.after(0, self._show_window)

            def on_quit(icon, item):
                icon.stop()
                self.root.after(0, self._quit)

            menu = pystray.Menu(
                pystray.MenuItem("表示", on_show, default=True),
                pystray.Menu.SEPARATOR,
                pystray.MenuItem("終了", on_quit),
            )
            self.tray_icon = pystray.Icon("EDINET Monitor", img, "EDINET Monitor", menu)
            self._tray_available = True
        except ImportError:
            self._tray_available = False
        except Exception as e:
            self._tray_available = False
            logger.warning("トレイアイコン初期化失敗: %s", e)

    def _setup_copy_support(self):
        """マウス/キーボードでのコピー操作を設定."""
        self.copy_menu = tk.Menu(self.root, tearoff=0)
        self.copy_menu.add_command(label="行をコピー", command=self._copy_selected_row)
        self.copy_menu.add_command(label="セルをコピー", command=self._copy_cell_from_menu)
        self.tree.bind("<Button-3>", self._show_tree_context_menu)
        self._menu_copy_column = None

    # ===== イベントハンドラ =====

    def _on_tree_select(self, event):
        selection = self.tree.selection()
        if not selection:
            return
        doc_id = selection[0]
        doc = self._doc_cache.get(doc_id)
        if doc:
            self._show_detail(doc)

    def _on_tree_double_click(self, event):
        self._toggle_read()

    def _show_tree_context_menu(self, event):
        row_id = self.tree.identify_row(event.y)
        if not row_id:
            return
        self.tree.selection_set(row_id)
        self._menu_copy_column = self.tree.identify_column(event.x)
        self.copy_menu.tk_popup(event.x_root, event.y_root)

    def _copy_cell_from_menu(self):
        self._copy_selected_row(column=self._menu_copy_column)

    def _copy_selected_row(self, event=None, column: str = None):
        selection = self.tree.selection()
        if not selection:
            return "break"
        row = self.tree.item(selection[0], "values")
        if not row:
            return "break"

        if column and column.startswith("#"):
            idx = int(column[1:]) - 1
            text = str(row[idx]) if 0 <= idx < len(row) else "\t".join(str(v) for v in row)
        else:
            text = "\t".join(str(v) for v in row)

        self.root.clipboard_clear()
        self.root.clipboard_append(text)
        self._update_status("running", "コピーしました")
        return "break"

    def _refresh_copy_blob(self):
        """詳細＋重要情報をまとめたコピー用テキストを更新."""
        if not self.selected_doc:
            text = ""
        else:
            doc = self.selected_doc
            target = doc.target_display
            lines = [
                f"[{doc.event_category}] {doc.filer_name}",
                f"書類名: {doc.doc_description}",
                f"提出時刻: {doc.submit_datetime}",
            ]
            if target:
                lines.append(f"対象: {target}")
            if doc.current_report_reason:
                lines.append(f"提出理由: {doc.current_report_reason}")
            lines.append("")
            lines.append("重要情報:")
            if self._current_xbrl_data:
                for key, value in self._current_xbrl_data.items():
                    lines.append(f"  {key}: {value}")
            else:
                lines.append("  -")
            text = "\n".join(lines)

        self.copy_blob_text.configure(state=tk.NORMAL)
        self.copy_blob_text.delete("1.0", tk.END)
        self.copy_blob_text.insert("1.0", text)
        self.copy_blob_text.configure(state=tk.DISABLED)

    def _on_filter_change(self):
        self.filter_category = self.category_var.get()
        self.filter_read = self.read_filter_var.get()
        self.search_text = self.search_var.get().strip()
        self._refresh_list()

    def _prev_date(self):
        self.view_date -= timedelta(days=1)
        self._update_date_display()
        self._refresh_list()

    def _next_date(self):
        self.view_date += timedelta(days=1)
        self._update_date_display()
        self._refresh_list()

    def _today_date(self):
        self.view_date = date.today()
        self._update_date_display()
        self._refresh_list()

    def _update_date_display(self):
        self.date_btn_var.set(self.view_date.strftime("%m/%d"))
        self.date_label.config(text=f"日付: {self.view_date.strftime('%Y-%m-%d')}")

    def _on_close(self):
        if self._tray_available:
            self.root.withdraw()
            self.tray_icon.visible = True
        else:
            self._quit()

    def _show_window(self):
        self.root.deiconify()
        self.root.lift()
        self.root.focus_force()

    def _quit(self):
        self.monitor.stop()
        if hasattr(self, "screen_monitor") and self.screen_monitor:
            self.screen_monitor.stop()
        if self._tray_available:
            try:
                self.tray_icon.stop()
            except Exception:
                pass
        self.root.destroy()

    # ===== 一覧操作 =====

    def _refresh_list(self):
        category = None if self.filter_category == ALL_CATEGORIES else self.filter_category
        is_read = None
        if self.filter_read == "unread":
            is_read = False
        elif self.filter_read == "read":
            is_read = True

        search = self.search_text if self.search_text else None

        docs = self.storage.get_documents(
            date=self.view_date.strftime("%Y-%m-%d"),
            category=category,
            is_read=is_read,
            search_text=search,
            sort_by=self.sort_var.get(),
        )

        # API検出済みdoc_idのセット（速報行の重複排除用）
        api_doc_ids = {doc.doc_id for doc in docs}

        # 速報行のうち、まだAPIで検出されていないものを保持
        surviving_screen_docs: list[Document] = []
        for iid, doc in list(self._screen_docs.items()):
            # submit_datetimeとedinet_codeでAPI側と照合
            matched = False
            for api_doc in docs:
                if (api_doc.edinet_code == doc.edinet_code
                        and api_doc.submit_datetime[:16] == doc.submit_datetime[:16]
                        and api_doc.doc_description == doc.doc_description):
                    matched = True
                    break
            if not matched:
                surviving_screen_docs.append(doc)

        # キャッシュ更新
        self._doc_cache = {doc.doc_id: doc for doc in docs}
        for doc in surviving_screen_docs:
            self._doc_cache[doc.doc_id] = doc

        self.tree.delete(*self.tree.get_children())

        # お気に入りフィルター
        star_only = self.star_filter_var.get()

        # 速報行を先頭に挿入
        for doc in surviving_screen_docs:
            cat_display = doc.event_category
            if doc.tag:
                cat_display += f"({doc.tag})"
            tags = ["unread", f"p{doc.priority}", "screen_flash"]
            self.tree.insert("", tk.END, iid=doc.doc_id, values=(
                "",
                doc.submit_time,
                cat_display,
                doc.priority_label,
                doc.target_ticker,
                doc.target_name,
                f"[速報] {doc.filer_name}",
            ), tags=tags)

        # DB上の書類を挿入
        for doc in docs:
            if star_only and not doc.is_starred:
                continue

            tags = []
            if not doc.is_read:
                tags.append("unread")
            else:
                tags.append("read")
            tags.append(f"p{doc.priority}")

            cat_display = doc.event_category
            if doc.tag:
                cat_display += f"({doc.tag})"

            self.tree.insert("", tk.END, iid=doc.doc_id, values=(
                "★" if doc.is_starred else "",
                doc.submit_time,
                cat_display,
                doc.priority_label,
                doc.target_ticker,
                doc.target_name,
                doc.filer_name,
            ), tags=tags)

        unread = self.storage.get_unread_count() + len(surviving_screen_docs)
        self.unread_label.config(text=f"未読: {unread}")
        if unread > 0:
            self.root.title(f"EDINET Monitor ({unread})")
        else:
            self.root.title("EDINET Monitor")

    def _mark_all_read(self):
        self.storage.mark_all_read()
        self._refresh_list()
        if self.selected_doc:
            self.selected_doc.is_read = True
            self._update_read_button()

    # ===== 詳細表示 =====

    def _show_detail(self, doc: Document):
        self.selected_doc = doc

        # ヘッダー
        header = f"[{doc.event_category}] {doc.filer_name}"
        target = doc.target_display
        if target:
            header += f" → {target}"
        if doc.doc_description:
            header += f"\n{doc.doc_description}"
        self.detail_header.config(text=header)

        self._current_xbrl_data = None
        self._refresh_copy_blob()

        self.memo_text.delete("1.0", tk.END)
        if doc.memo:
            self.memo_text.insert("1.0", doc.memo)

        is_screen = doc.doc_id.startswith("screen_")
        self.btn_pdf.config(state=tk.NORMAL if doc.pdf_flag and not is_screen else tk.DISABLED, text="PDF表示")
        self.btn_edinet.config(state=tk.NORMAL, text="EDINET検索" if is_screen else "EDINET原文")
        self.btn_toggle_read.config(state=tk.NORMAL if not is_screen else tk.DISABLED)
        self.btn_raw.config(state=tk.NORMAL if doc.raw_json and not is_screen else tk.DISABLED)
        self.btn_save_memo.config(state=tk.NORMAL if not is_screen else tk.DISABLED)

        self._update_read_button()
        self._update_star_button()

        # XBRL情報の非同期取得
        if doc.xbrl_flag:
            self._load_xbrl_info(doc)
        else:
            self._clear_xbrl_info("XBRLデータなし")

    def _load_xbrl_info(self, doc: Document):
        """XBRL情報を非同期で取得・表示."""
        self._clear_xbrl_info("読み込み中...")

        def fetch():
            if not hasattr(self, '_xbrl_parser'):
                from xbrl_parser import XbrlParser
                api_key = self.config.get("edinet", {}).get("api_key", "")
                self._xbrl_parser = XbrlParser(api_key)
            result = self._xbrl_parser.extract(doc.doc_id, doc.event_category)
            self.msg_queue.put(("xbrl_result", doc.doc_id, result))

        threading.Thread(target=fetch, daemon=True).start()

    def _clear_xbrl_info(self, message: str = ""):
        """XBRL情報表示をクリア."""
        for widget in self.xbrl_frame.winfo_children():
            widget.destroy()
        if message:
            ttk.Label(self.xbrl_frame, text=message, foreground="#888").pack(anchor=tk.W)
        self._current_xbrl_data = None
        self._refresh_copy_blob()

    def _show_xbrl_info(self, data: dict):
        """XBRL抽出結果を表示."""
        for widget in self.xbrl_frame.winfo_children():
            widget.destroy()

        if not data:
            ttk.Label(self.xbrl_frame, text="抽出データなし", foreground="#888").pack(anchor=tk.W)
            self._current_xbrl_data = None
            self._refresh_copy_blob()
            return

        for key, value in data.items():
            row_frame = ttk.Frame(self.xbrl_frame)
            row_frame.pack(fill=tk.X, pady=1)

            # ラベル
            lbl = ttk.Label(row_frame, text=f"{key}:", font=(self.jp_font, 10, "bold"), width=14, anchor=tk.E)
            lbl.pack(side=tk.LEFT, padx=(0, 4))

            # 値 (重要なものは赤色)
            fg = "#333"
            if key in ("増減", "希薄化率") and value.startswith("-"):
                fg = "#1565C0"  # 減少 = 青
            elif key in ("増減", "希薄化率"):
                fg = "#D32F2F"  # 増加 = 赤
            elif "⚠" in value:
                fg = "#D32F2F"

            val = ttk.Label(row_frame, text=value, foreground=fg, wraplength=350)
            val.pack(side=tk.LEFT)
        self._current_xbrl_data = data
        self._refresh_copy_blob()

    def _update_read_button(self):
        if self.selected_doc:
            text = "未読に戻す" if self.selected_doc.is_read else "既読にする"
            self.btn_toggle_read.config(text=text)

    def _update_star_button(self):
        if self.selected_doc:
            if self.selected_doc.doc_id.startswith("screen_"):
                self.btn_star.config(text="★ お気に入り", state=tk.DISABLED)
            elif self.selected_doc.is_starred:
                self.btn_star.config(text="★ 解除", state=tk.NORMAL)
            else:
                self.btn_star.config(text="☆ お気に入り", state=tk.NORMAL)

    def _toggle_read(self):
        if not self.selected_doc:
            return
        new_status = not self.selected_doc.is_read
        self.storage.update_read_status(self.selected_doc.doc_id, new_status)
        self.selected_doc.is_read = new_status
        self._update_read_button()
        self._refresh_list()
        if self.tree.exists(self.selected_doc.doc_id):
            self.tree.selection_set(self.selected_doc.doc_id)

    def _toggle_star(self):
        if not self.selected_doc:
            return
        # screen_で始まるIDはDB未保存なのでスター不可
        if self.selected_doc.doc_id.startswith("screen_"):
            return
        new_status = not self.selected_doc.is_starred
        self.storage.update_starred(self.selected_doc.doc_id, new_status)
        self.selected_doc.is_starred = new_status
        self._update_star_button()
        self._refresh_list()
        if self.tree.exists(self.selected_doc.doc_id):
            self.tree.selection_set(self.selected_doc.doc_id)

    def _save_memo(self):
        if not self.selected_doc:
            return
        memo = self.memo_text.get("1.0", tk.END).strip()
        self.storage.update_memo(self.selected_doc.doc_id, memo)
        self.selected_doc.memo = memo

    def _open_pdf(self):
        if not self.selected_doc:
            return
        doc = self.selected_doc
        cache_dir = Path(PDF_CACHE_DIR)
        cache_dir.mkdir(parents=True, exist_ok=True)
        pdf_path = cache_dir / f"{doc.doc_id}.pdf"

        if pdf_path.exists():
            self._launch_file(str(pdf_path))
            return

        self.btn_pdf.config(state=tk.DISABLED, text="DL中...")

        def download():
            success = self.monitor.download_pdf(doc.doc_id, str(pdf_path))
            self.msg_queue.put(("pdf_done", doc.doc_id, success, str(pdf_path)))

        threading.Thread(target=download, daemon=True).start()

    def _open_edinet(self):
        """EDINET原文をブラウザで開く."""
        if not self.selected_doc:
            return
        doc_id = self.selected_doc.doc_id
        if doc_id.startswith("S100"):
            # EDINET書類閲覧ページ
            url = f"https://disclosure2.edinet-fsa.go.jp/WZEK0040.aspx?{doc_id}"
        else:
            # 速報行: EDINET書類検索ページを開く（当日の開示一覧にすぐアクセスできる）
            url = "https://disclosure2.edinet-fsa.go.jp/WEEK0010.aspx"
        webbrowser.open(url)

    def _show_raw_json(self):
        if not self.selected_doc or not self.selected_doc.raw_json:
            return

        win = tk.Toplevel(self.root)
        win.title(f"生データ - {self.selected_doc.doc_id}")
        win.geometry("600x500")
        win.transient(self.root)

        text = tk.Text(win, wrap=tk.WORD, font=("Consolas", 10))
        scrollbar = ttk.Scrollbar(win, orient=tk.VERTICAL, command=text.yview)
        text.configure(yscrollcommand=scrollbar.set)

        try:
            formatted = json.dumps(
                json.loads(self.selected_doc.raw_json), indent=2, ensure_ascii=False
            )
        except Exception:
            formatted = self.selected_doc.raw_json

        text.insert("1.0", formatted)
        text.config(state=tk.DISABLED)

        text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    def _launch_file(self, path: str):
        try:
            os.startfile(path)
        except Exception as e:
            messagebox.showerror("エラー", f"ファイルを開けません:\n{e}")

    # ===== 監視制御 =====

    def _toggle_monitor(self):
        if self.monitor.is_running:
            self.monitor.stop()
            if hasattr(self, "screen_monitor") and self.screen_monitor:
                self.screen_monitor.stop()
            self.monitor_btn_text.set("監視開始")
        else:
            self.monitor.start()
            if hasattr(self, "screen_monitor") and self.screen_monitor:
                self.screen_monitor.start()
            self.monitor_btn_text.set("監視停止")

    def _manual_poll(self):
        def poll():
            self.monitor.poll_once()
        threading.Thread(target=poll, daemon=True).start()

    # ===== メッセージキュー処理 =====

    def enqueue_new_docs(self, docs: list[Document]):
        self.msg_queue.put(("new_docs", docs))

    def enqueue_screen_docs(self, screen_observations: list[dict]):
        self.msg_queue.put(("screen_docs", screen_observations))

    def enqueue_status(self, status: str, message: str):
        self.msg_queue.put(("status", status, message))

    def _process_queue(self):
        try:
            while True:
                msg = self.msg_queue.get_nowait()
                self._handle_message(msg)
        except queue.Empty:
            pass
        self.root.after(100, self._process_queue)

    def _handle_message(self, msg):
        msg_type = msg[0]

        if msg_type == "new_docs":
            docs = msg[1]
            # API検出時、速報行があれば削除（正式データで置き換え）
            for doc in docs:
                screen_iid = f"screen_{doc.edinet_code}_{doc.submit_datetime[:16]}"
                if self.tree.exists(screen_iid):
                    self.tree.delete(screen_iid)
                    self._doc_cache.pop(screen_iid, None)
                self._screen_docs.pop(screen_iid, None)
            queue_received_at = datetime.now().isoformat()
            for doc in docs:
                self.storage.record_document_event(doc, "gui_queue_received", event_at=queue_received_at)
            self._refresh_list()
            notify_started_at = datetime.now().isoformat()
            for doc in docs:
                self.storage.record_document_event(doc, "notification_started", event_at=notify_started_at)
            # 速報通知済みのカテゴリは重複通知しない
            docs_to_notify = []
            for doc in docs:
                screen_iid = f"screen_{doc.edinet_code}_{doc.submit_datetime[:16]}"
                if screen_iid not in self._screen_notified:
                    docs_to_notify.append(doc)
                else:
                    self._screen_notified.discard(screen_iid)
            completion_times = self.notifier.notify_batch(docs_to_notify)
            for doc in docs_to_notify:
                completed_at = completion_times.get(doc.doc_id)
                if completed_at:
                    self.storage.record_document_event(doc, "notification_completed", event_at=completed_at)
            if not self.root.winfo_viewable():
                self._flash_taskbar()

        elif msg_type == "screen_docs":
            self._handle_screen_docs(msg[1])

        elif msg_type == "status":
            status, message = msg[1], msg[2]
            self._update_status(status, message)

        elif msg_type == "pdf_done":
            doc_id, success, pdf_path = msg[1], msg[2], msg[3]
            self.btn_pdf.config(text="PDF表示")
            if self.selected_doc and self.selected_doc.doc_id == doc_id:
                self.btn_pdf.config(state=tk.NORMAL)
            if success:
                self._launch_file(pdf_path)
            else:
                messagebox.showerror("エラー", "PDFのダウンロードに失敗しました。")

        elif msg_type == "xbrl_result":
            doc_id, data = msg[1], msg[2]
            if self.selected_doc and self.selected_doc.doc_id == doc_id:
                self._show_xbrl_info(data)

    def _handle_screen_docs(self, observations: list[dict]):
        """スクレイピングで新規検出された書類を速報としてGUI表示+通知."""
        mon_cfg = self.config.get("monitoring", {})
        classifier = Classifier(
            enabled_categories=mon_cfg.get("enabled_categories"),
            skip_corrections=mon_cfg.get("skip_corrections", True),
        )
        watchlist = mon_cfg.get("watchlist_sec_codes", [])

        docs_to_notify: list[Document] = []
        for obs in observations:
            submit_dt = obs.get("submit_datetime", "")
            doc_desc = obs.get("doc_description", "")
            edinet_code = obs.get("edinet_code", "")
            filer_name = obs.get("filer_name", "")
            target_text = obs.get("target_text", "")
            screen_iid = f"screen_{edinet_code}_{submit_dt[:16]}"

            # 既にTreeviewにある速報行ならスキップ
            if self.tree.exists(screen_iid):
                continue

            # 既にAPIで検出済みかチェック（doc_cacheのedinet_code+submit+descで照合）
            already_in_api = False
            for cached_doc in self._doc_cache.values():
                if (cached_doc.edinet_code == edinet_code
                        and cached_doc.submit_datetime[:16] == submit_dt[:16]
                        and cached_doc.doc_description == doc_desc):
                    already_in_api = True
                    break
            if already_in_api:
                logger.debug("screen_skip: already in API %s %s", edinet_code, doc_desc)
                continue

            # classifier用の仮Documentを作成
            temp_doc = Document(
                doc_id=screen_iid,
                edinet_code=edinet_code,
                sec_code=None,
                filer_name=filer_name,
                doc_type_code="",
                doc_description=doc_desc,
                submit_datetime=submit_dt,
                ordinance_code="",
                form_code="",
            )

            # 対象会社情報を解決
            result = self.storage.lookup_edinet_code(edinet_code)
            if result:
                temp_doc.sec_code = result[1]

            # 分類
            category, priority, tag = classifier.classify_with_tag(temp_doc)
            if category == "その他":
                continue

            priority = classifier.adjust_priority_for_watchlist(
                priority, temp_doc.sec_code or "", watchlist,
            )

            temp_doc.event_category = category
            temp_doc.priority = priority
            temp_doc.tag = tag

            # target_textから対象会社情報を補完
            if target_text:
                temp_doc.issuer_name = target_text
                # 対象会社名からsec_code(ティッカー)を逆引き
                target_lookup = self.storage.lookup_by_company_name(target_text)
                if target_lookup:
                    temp_doc.issuer_edinet_code = target_lookup[0]
                    temp_doc.issuer_sec_code = target_lookup[1] or ""
                    logger.debug("screen_target_resolved: %s → %s", target_text, target_lookup[1])
                else:
                    logger.info("screen_target_unresolved: '%s' not found in edinet_codes", target_text)

            # Treeviewに速報行を挿入（先頭に）
            cat_display = category
            if tag:
                cat_display += f"({tag})"

            submit_time = submit_dt.split(" ", 1)[1][:5] if " " in submit_dt else submit_dt[:5]

            target_display = temp_doc.target_display or ""
            tags = ["unread", f"p{priority}", "screen_flash"]

            self.tree.insert("", 0, iid=screen_iid, values=(
                "",
                submit_time,
                cat_display,
                temp_doc.priority_label,
                temp_doc.target_ticker,
                temp_doc.target_name,
                f"[速報] {filer_name}",
            ), tags=tags)
            self._doc_cache[screen_iid] = temp_doc
            self._screen_docs[screen_iid] = temp_doc

            docs_to_notify.append(temp_doc)
            logger.info(
                "screen_flash: %s %s %s priority=%d",
                category, filer_name, doc_desc, priority,
            )

        # 通知
        if docs_to_notify:
            for doc in docs_to_notify:
                logger.info("screen_notify: %s %s priority=%d", doc.filer_name, doc.doc_description, doc.priority)
            self.notifier.notify_batch(docs_to_notify)
            for doc in docs_to_notify:
                self._screen_notified.add(doc.doc_id)

            # ウィンドウを前面に
            if not self.root.winfo_viewable():
                self._flash_taskbar()

            # 未読カウント更新
            unread = self.storage.get_unread_count() + len(docs_to_notify)
            self.unread_label.config(text=f"未読: {unread}")
            self.root.title(f"EDINET Monitor ({unread})")

    def _update_status(self, status: str, message: str):
        colors = {
            "running": "#4CAF50",
            "polling": "#FF9800",
            "stopped": "#9E9E9E",
            "error": "#F44336",
        }
        color = colors.get(status, "#9E9E9E")
        self.status_icon.config(foreground=color)
        self.status_label.config(text=message)

    def _flash_taskbar(self):
        try:
            self.root.deiconify()
            self.root.lift()
        except Exception:
            pass

    # ===== 起動 =====

    def run(self):
        if self._tray_available:
            threading.Thread(target=self.tray_icon.run, daemon=True).start()

        self.monitor.start()
        if hasattr(self, "screen_monitor") and self.screen_monitor:
            self.screen_monitor.start()
        self.root.mainloop()
