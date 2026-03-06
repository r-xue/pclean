/* zoom.js — interactive zoom/pan for figures and mermaid diagrams
 *
 * Vendored dependencies (loaded before this file via conf.py html_js_files):
 *   vendor/medium-zoom.min.js  v1.1.0  (MIT) — click-to-zoom lightbox for <img>
 *   vendor/panzoom.min.js      v4.5.1  (MIT) — scroll/drag/click zoom for SVGs
 *
 * Mermaid UX:
 *   click diagram — open full-screen modal
 *   inside modal:  scroll-wheel to zoom, drag to pan, left-click to zoom in,
 *                  right-click to zoom out, [⟳] button or Escape to close/reset
 */
document.addEventListener('DOMContentLoaded', function () {

  // ── medium-zoom for regular <img> elements ────────────────────────────
  mediumZoom('article img', { background: 'rgba(0,0,0,0.85)', margin: 40 });

  // ── mermaid modal with panzoom ────────────────────────────────────────────
  (function () {
    var ZOOM_STEP = 1.4;
    var DRAG_THRESHOLD = 5;

    // --- build the modal DOM (created once, reused) ---
    var overlay = document.createElement('div');
    overlay.id = 'mermaid-modal-overlay';

    var modalBox = document.createElement('div');
    modalBox.id = 'mermaid-modal-box';

    var closeBtn = document.createElement('button');
    closeBtn.id = 'mermaid-modal-close';
    closeBtn.textContent = '✕';
    closeBtn.title = 'Close (Esc)';

    var resetBtn = document.createElement('button');
    resetBtn.id = 'mermaid-modal-reset';
    resetBtn.textContent = '⟳';
    resetBtn.title = 'Reset zoom';

    var hint = document.createElement('div');
    hint.id = 'mermaid-modal-hint';
    hint.textContent = 'scroll to zoom · drag to pan · left-click zoom in · right-click zoom out';

    overlay.appendChild(closeBtn);
    overlay.appendChild(resetBtn);
    overlay.appendChild(hint);
    overlay.appendChild(modalBox);
    document.body.appendChild(overlay);

    var activePz = null;

    function closeModal() {
      overlay.classList.remove('visible');
      if (activePz) { activePz.destroy(); activePz = null; }
      while (modalBox.firstChild) { modalBox.removeChild(modalBox.firstChild); }
    }

    function openModal(sourceSvg) {
      // Clone the SVG so the original stays intact
      var clone = sourceSvg.cloneNode(true);
      clone.removeAttribute('style');
      clone.style.width = '100%';
      clone.style.height = '100%';
      modalBox.appendChild(clone);

      overlay.classList.add('visible');

      activePz = Panzoom(clone, { maxScale: 10, contain: 'outside' });

      // scroll-wheel zoom
      modalBox.addEventListener('wheel', activePz.zoomWithWheel);

      // click-to-zoom (left = in, right = out)
      var downX, downY;
      clone.addEventListener('mousedown', function (e) { downX = e.clientX; downY = e.clientY; });
      clone.addEventListener('mouseup', function (e) {
        var d = Math.sqrt(Math.pow(e.clientX - downX, 2) + Math.pow(e.clientY - downY, 2));
        if (d < DRAG_THRESHOLD) {
          if (e.button === 0) activePz.zoomIn({ step: ZOOM_STEP });
          if (e.button === 2) activePz.zoomOut({ step: ZOOM_STEP });
        }
      });
      clone.addEventListener('contextmenu', function (e) { e.preventDefault(); });

      resetBtn.onclick = function () { activePz.reset(); };
    }

    // close on overlay background click, close button, or Escape
    overlay.addEventListener('click', function (e) {
      if (e.target === overlay) closeModal();
    });
    closeBtn.addEventListener('click', closeModal);
    document.addEventListener('keydown', function (e) {
      if (e.key === 'Escape' && overlay.classList.contains('visible')) closeModal();
    });

    // --- wire up each mermaid diagram ---
    function initMermaid() {
      document.querySelectorAll('.mermaid > svg').forEach(function (svg) {
        if (svg.dataset.mermaidModal) return;
        svg.dataset.mermaidModal = '1';
        svg.style.cursor = 'zoom-in';
        svg.addEventListener('click', function (e) {
          e.stopPropagation();
          openModal(svg);
        });
      });
    }

    initMermaid();
    setTimeout(initMermaid, 500);
    setTimeout(initMermaid, 1500);
  })();
});

