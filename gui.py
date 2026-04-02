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
import subprocess
import sys
import threading
import tkinter as tk
import webbrowser
from datetime import date, datetime, timedelta
from pathlib import Path
from tkinter import font as tkfont
from tkinter import messagebox, ttk
from typing import Optional

from config import PDF_CACHE_DIR
from models import Document, EventCategory, Priority, PRIORITY_LABELS

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
        paned.add(left_frame, weight=2)
        self._create_event_list(left_frame)

        right_frame = ttk.Frame(paned)
        paned.add(right_frame, weight=3)
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
        columns = ("time", "category", "pri", "target", "filer", "description")
        self.tree = ttk.Treeview(parent, columns=columns, show="headings", selectmode="browse")

        self.tree.heading("time", text="時刻", anchor=tk.W)
        self.tree.heading("category", text="種別", anchor=tk.W)
        self.tree.heading("pri", text="優先", anchor=tk.CENTER)
        self.tree.heading("target", text="対象", anchor=tk.W)
        self.tree.heading("filer", text="提出者", anchor=tk.W)
        self.tree.heading("description", text="書類名", anchor=tk.W)

        self.tree.column("time", width=50, minwidth=45, stretch=False)
        self.tree.column("category", width=100, minwidth=70, stretch=False)
        self.tree.column("pri", width=40, minwidth=35, stretch=False, anchor=tk.CENTER)
        self.tree.column("target", width=180, minwidth=100)
        self.tree.column("filer", width=150, minwidth=100)
        self.tree.column("description", width=130, minwidth=80)

        self.tree.tag_configure("unread", font=(self.jp_font, 10, "bold"))
        self.tree.tag_configure("read", foreground="#888888")
        # 優先度: テキスト色 + 背景色
        self.tree.tag_configure("p1", foreground="#D32F2F", background="#FFF3F3")
        self.tree.tag_configure("p2", foreground="#E65100", background="#FFF8E1")
        self.tree.tag_configure("p3", foreground="#1565C0", background="#F5F5F5")
        self.tree.tag_configure("p4", foreground="#9E9E9E", background="#FAFAFA")

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
        pad = {"padx": 8, "pady": 2, "sticky": tk.W}

        self.detail_header = ttk.Label(f, text="書類を選択してください", style="Header.TLabel")
        self.detail_header.grid(row=0, column=0, columnspan=2, padx=8, pady=(8, 4), sticky=tk.W)

        ttk.Separator(f, orient=tk.HORIZONTAL).grid(row=1, column=0, columnspan=2, sticky=tk.EW, padx=8, pady=4)

        fields = [
            ("提出時刻:", "submit_time"),
            ("提出者:", "filer"),
            ("提出者コード:", "filer_ticker"),
            ("発行会社:", "issuer"),
            ("対象会社:", "subject"),
            ("子会社:", "subsidiary"),
            ("書類名:", "description"),
            ("イベント分類:", "category"),
            ("優先度:", "priority"),
            ("EDINET コード:", "edinet_code"),
            ("府令/様式:", "ordinance_form"),
        ]

        self.detail_labels = {}
        multiline_heights = {
            "submit_time": 2,
            "filer": 2,
            "filer_ticker": 2,
            "issuer": 2,
            "subject": 2,
            "subsidiary": 2,
            "description": 3,
            "category": 2,
            "priority": 2,
            "edinet_code": 2,
            "ordinance_form": 2,
        }
        for i, (label_text, key) in enumerate(fields, start=2):
            ttk.Label(f, text=label_text, font=(self.jp_font, 10, "bold")).grid(
                row=i, column=0, padx=(8, 4), pady=2, sticky=tk.NE
            )
            txt = tk.Text(
                f,
                height=multiline_heights.get(key, 1),
                width=52,
                wrap=tk.WORD,
                font=(self.jp_font, 10),
                relief=tk.FLAT,
                highlightthickness=0,
                borderwidth=0,
            )
            txt.grid(row=i, column=1, **pad)
            txt.configure(state=tk.DISABLED)
            self.detail_labels[key] = txt

        row = len(fields) + 2

        # 提出理由
        ttk.Label(f, text="提出理由:", font=(self.jp_font, 10, "bold")).grid(
            row=row, column=0, padx=(8, 4), pady=2, sticky=tk.NE
        )
        self.detail_reason = tk.Text(
            f,
            height=3,
            width=52,
            wrap=tk.WORD,
            font=(self.jp_font, 10),
            relief=tk.FLAT,
            highlightthickness=0,
            borderwidth=0,
        )
        self.detail_reason.grid(row=row, column=1, **pad)
        self.detail_reason.configure(state=tk.DISABLED)
        row += 1

        ttk.Separator(f, orient=tk.HORIZONTAL).grid(row=row, column=0, columnspan=2, sticky=tk.EW, padx=8, pady=8)
        row += 1

        # アクションボタン
        btn_frame = ttk.Frame(f)
        btn_frame.grid(row=row, column=0, columnspan=2, padx=8, pady=4, sticky=tk.W)

        self.btn_pdf = ttk.Button(btn_frame, text="PDF表示", command=self._open_pdf, state=tk.DISABLED)
        self.btn_pdf.pack(side=tk.LEFT, padx=(0, 4))

        self.btn_edinet = ttk.Button(btn_frame, text="EDINET原文", command=self._open_edinet, state=tk.DISABLED)
        self.btn_edinet.pack(side=tk.LEFT, padx=(0, 4))

        self.btn_toggle_read = ttk.Button(btn_frame, text="既読にする", command=self._toggle_read, state=tk.DISABLED)
        self.btn_toggle_read.pack(side=tk.LEFT, padx=(0, 4))

        self.btn_raw = ttk.Button(btn_frame, text="生データ", command=self._show_raw_json, state=tk.DISABLED)
        self.btn_raw.pack(side=tk.LEFT, padx=(0, 4))

        row += 1

        ttk.Separator(f, orient=tk.HORIZONTAL).grid(row=row, column=0, columnspan=2, sticky=tk.EW, padx=8, pady=8)
        row += 1

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

        # XBRL抽出情報
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

    def _set_detail_text(self, key: str, value: str, fg: str = "#222222"):
        widget = self.detail_labels[key]
        widget.configure(state=tk.NORMAL, foreground=fg)
        widget.delete("1.0", tk.END)
        widget.insert("1.0", value or "")
        widget.configure(state=tk.DISABLED)

    def _set_reason_text(self, value: str):
        self.detail_reason.configure(state=tk.NORMAL, foreground="#222222")
        self.detail_reason.delete("1.0", tk.END)
        self.detail_reason.insert("1.0", value or "")
        self.detail_reason.configure(state=tk.DISABLED)

    def _get_detail_value(self, key: str, fallback: str = "-") -> str:
        value = self.detail_labels[key].get("1.0", tk.END).strip()
        return value or fallback

    def _refresh_copy_blob(self):
        """詳細＋重要情報をまとめたコピー用テキストを更新."""
        if not self.selected_doc:
            text = ""
        else:
            doc = self.selected_doc
            lines = [
                f"提出時刻: {self._get_detail_value('submit_time')}",
                f"提出者: {self._get_detail_value('filer')}",
                f"提出者コード: {self._get_detail_value('filer_ticker')}",
                f"発行会社: {self._get_detail_value('issuer')}",
                f"対象会社: {self._get_detail_value('subject')}",
                f"子会社: {self._get_detail_value('subsidiary')}",
                f"書類名: {self._get_detail_value('description')}",
                f"イベント分類: {self._get_detail_value('category')}",
                f"優先度: {self._get_detail_value('priority')}",
                f"EDINETコード: {self._get_detail_value('edinet_code')}",
                f"府令/様式: {self._get_detail_value('ordinance_form')}",
                f"提出理由: {doc.current_report_reason or '-'}",
                "",
                "重要情報:",
            ]
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

        # キャッシュ更新
        self._doc_cache = {doc.doc_id: doc for doc in docs}

        self.tree.delete(*self.tree.get_children())
        for doc in docs:
            tags = []
            if not doc.is_read:
                tags.append("unread")
            else:
                tags.append("read")
            tags.append(f"p{doc.priority}")

            # 対象会社の表示
            target = doc.target_display

            # カテゴリ+タグ表示
            cat_display = doc.event_category
            if doc.tag:
                cat_display += f"({doc.tag})"

            self.tree.insert("", tk.END, iid=doc.doc_id, values=(
                doc.submit_time,
                cat_display,
                doc.priority_label,
                target,
                doc.filer_name,
                doc.doc_description,
            ), tags=tags)

        unread = self.storage.get_unread_count()
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
        self.detail_header.config(text=header)

        # 各フィールド
        self._set_detail_text("submit_time", doc.submit_datetime)
        self._set_detail_text("filer", doc.filer_name)
        self._set_detail_text("filer_ticker", doc.ticker or "-")

        # 発行会社
        issuer_text = "-"
        if doc.issuer_name:
            issuer_text = doc.issuer_name
            if doc.issuer_sec_code:
                issuer_text += f" ({doc.issuer_sec_code[:4]})"
        elif doc.issuer_edinet_code:
            issuer_text = f"[{doc.issuer_edinet_code}]"
        self._set_detail_text("issuer", issuer_text)

        # 対象会社
        subject_text = "-"
        if doc.subject_name:
            subject_text = doc.subject_name
            if doc.subject_sec_code:
                subject_text += f" ({doc.subject_sec_code[:4]})"
        elif doc.subject_edinet_code:
            subject_text = f"[{doc.subject_edinet_code}]"
        self._set_detail_text("subject", subject_text)

        # 子会社
        sub_text = doc.subsidiary_name or "-"
        if not doc.subsidiary_name and doc.subsidiary_edinet_code:
            sub_text = f"[{doc.subsidiary_edinet_code}]"
        self._set_detail_text("subsidiary", sub_text)

        self._set_detail_text("description", doc.doc_description)
        self._set_detail_text("category", doc.event_category)
        self._set_detail_text("edinet_code", doc.edinet_code or "-")
        self._set_detail_text("ordinance_form", f"{doc.ordinance_code or '-'} / {doc.form_code or '-'}")

        p_label = doc.priority_label
        p_color = PRIORITY_COLORS.get(doc.priority, "#616161")
        self._set_detail_text("priority", p_label, fg=p_color)

        self._set_reason_text(doc.current_report_reason or "-")
        self._current_xbrl_data = None
        self._refresh_copy_blob()

        self.memo_text.delete("1.0", tk.END)
        if doc.memo:
            self.memo_text.insert("1.0", doc.memo)

        self.btn_pdf.config(state=tk.NORMAL if doc.pdf_flag else tk.DISABLED, text="PDF表示")
        self.btn_edinet.config(state=tk.NORMAL)
        self.btn_toggle_read.config(state=tk.NORMAL)
        self.btn_raw.config(state=tk.NORMAL if doc.raw_json else tk.DISABLED)
        self.btn_save_memo.config(state=tk.NORMAL)

        self._update_read_button()

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
        # EDINET書類閲覧ページ
        url = f"https://disclosure2.edinet-fsa.go.jp/WZEK0040.aspx?S100{doc_id[4:]}" if doc_id.startswith("S100") else None
        if not url:
            # フォールバック: EDINET検索ページ
            url = "https://disclosure.edinet-fsa.go.jp/"
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
            self.monitor_btn_text.set("監視開始")
        else:
            self.monitor.start()
            self.monitor_btn_text.set("監視停止")

    def _manual_poll(self):
        def poll():
            self.monitor.poll_once()
        threading.Thread(target=poll, daemon=True).start()

    # ===== メッセージキュー処理 =====

    def enqueue_new_docs(self, docs: list[Document]):
        self.msg_queue.put(("new_docs", docs))

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
            self._refresh_list()
            self.notifier.notify_batch(docs)
            if not self.root.winfo_viewable():
                self._flash_taskbar()

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
        self.root.mainloop()
