(function () {
  function hasFocusedField(selector) {
    var active = document.activeElement;
    return !!(active && active.matches && active.matches(selector));
  }

  function hasSelectedFile() {
    return Array.from(document.querySelectorAll('input[type="file"]')).some(function (input) {
      return input.files && input.files.length > 0;
    });
  }

  window.setupAutoRefresh = function setupAutoRefresh(options) {
    var config = options || {};
    var intervalMs = Number(config.intervalMs || 15000);
    var focusSelector = config.focusSelector || 'input, textarea, select';
    var pauseOnFocus = config.pauseOnFocus !== false;
    var pauseOnFileSelection = config.pauseOnFileSelection !== false;

    window.setInterval(function () {
      if (document.hidden) {
        return;
      }
      if (pauseOnFocus && hasFocusedField(focusSelector)) {
        return;
      }
      if (pauseOnFileSelection && hasSelectedFile()) {
        return;
      }
      window.location.reload();
    }, intervalMs);
  };
})();
