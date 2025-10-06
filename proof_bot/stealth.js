// proof_bot/stealth.js

// This script is injected into every page to hide the automation flags.

// Pass the webdriver check
Object.defineProperty(navigator, 'webdriver', {
  get: () => false,
});

// Pass the Chrome check
window.chrome = {
  runtime: {},
  // Add other properties if needed
};

// Pass the permissions check
const originalQuery = window.navigator.permissions.query;
window.navigator.permissions.query = (parameters) => (
  parameters.name === 'notifications'
    ? Promise.resolve({ state: Notification.permission })
    : originalQuery(parameters)
);

// Pass the plugins check
Object.defineProperty(navigator, 'plugins', {
  get: () => [
    { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
    { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: '' },
    { name: 'Native Client', filename: 'internal-nacl-plugin', description: '' },
  ],
});

// Pass the languages check
Object.defineProperty(navigator, 'languages', {
  get: () => ['en-US', 'en'],
});