const qrcode = require('qrcode');

module.exports = async (req, res) => {
  const host = req.headers.host || 'localhost:3000';
  const protocol = req.headers['x-forwarded-proto'] || 'http';
  const defaultUrl = `${protocol}://${host}`;
  const targetUrl = req.query.url || defaultUrl;

  try {
    const qrDataUrl = await qrcode.toDataURL(targetUrl, {
      margin: 2,
      width: 320,
      color: {
        dark: '#00f2fe',
        light: '#0b0f19'
      }
    });
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.status(200).json({ qrDataUrl, targetUrl });
  } catch (err) {
    res.status(500).json({ error: 'Failed to generate QR code' });
  }
};
