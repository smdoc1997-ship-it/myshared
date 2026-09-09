module.exports = (req, res) => {
  const code = Math.floor(100000 + Math.random() * 900000).toString();
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.status(200).json({ roomId: code });
};
