import nodemailer from 'nodemailer';
import dotenv from 'dotenv';

dotenv.config();

const transporter = nodemailer.createTransport({
  host: 'smtp.gmail.com',
  port: 587,
  secure: false,
  auth: {
    user: process.env.Email_username,
    pass: process.env.Email_password,
  },
});

/**
 * Send a CSAT notification email when a CSAT value is fetched for a record.
 * @param {Object} params
 * @param {string} params.agentName
 * @param {string} params.extension
 * @param {string} params.callerIdNumber
 * @param {string} params.calleeIdNumber
 * @param {string} params.csat
 * @param {string} [params.callId]
 */
export async function sendCSATEmail({ agentName, extension, callerIdNumber, calleeIdNumber, csat, callId }) {
  const to = process.env.Emails;
  const subject = `CSAT Alert – Score ${csat} | Agent: ${agentName || 'N/A'}`;

  const html = `
    <div style="font-family:Arial,sans-serif;max-width:520px;margin:0 auto;border:1px solid #e0e0e0;border-radius:8px;overflow:hidden;">
      <div style="background:#1a73e8;color:#fff;padding:16px 20px;">
        <h2 style="margin:0;font-size:18px;">CSAT Notification</h2>
      </div>
      <table style="width:100%;border-collapse:collapse;padding:16px;">
        <tr>
          <td style="padding:10px 20px;font-weight:bold;color:#555;border-bottom:1px solid #f0f0f0;">Agent Name</td>
          <td style="padding:10px 20px;border-bottom:1px solid #f0f0f0;">${agentName || 'N/A'}</td>
        </tr>
        <tr>
          <td style="padding:10px 20px;font-weight:bold;color:#555;border-bottom:1px solid #f0f0f0;">Extension</td>
          <td style="padding:10px 20px;border-bottom:1px solid #f0f0f0;">${extension || 'N/A'}</td>
        </tr>
        <tr>
          <td style="padding:10px 20px;font-weight:bold;color:#555;border-bottom:1px solid #f0f0f0;">Caller ID Number</td>
          <td style="padding:10px 20px;border-bottom:1px solid #f0f0f0;">${callerIdNumber || 'N/A'}</td>
        </tr>
        <tr>
          <td style="padding:10px 20px;font-weight:bold;color:#555;border-bottom:1px solid #f0f0f0;">Callee ID Number</td>
          <td style="padding:10px 20px;border-bottom:1px solid #f0f0f0;">${calleeIdNumber || 'N/A'}</td>
        </tr>
        <tr>
          <td style="padding:10px 20px;font-weight:bold;color:#555;">CSAT</td>
          <td style="padding:10px 20px;font-size:22px;font-weight:bold;color:#1a73e8;">${csat}</td>
        </tr>
      </table>
      ${callId ? `<div style="padding:8px 20px 4px;color:#999;font-size:12px;">Call ID: ${callId}</div>` : ''}
    </div>
    <div style="padding:12px 20px 16px;">
      <p style="margin:0;font-weight:bold;color:#e67e22;">Thank you, and have a great day!</p>
      <p style="margin:6px 0 0;font-weight:bold;color:#e67e22;">Team Multycomm</p>
    </div>
  `;

  try {
    const info = await transporter.sendMail({
      from: `"CDR CSAT Alert" <${process.env.Email_username}>`,
      to,
      subject,
      html,
    });
    console.log(`📧 CSAT email sent for call ${callId || 'unknown'}: ${info.messageId}`);
  } catch (err) {
    console.error(`❌ Failed to send CSAT email for call ${callId || 'unknown'}:`, err.message);
  }
}

export default { sendCSATEmail };
