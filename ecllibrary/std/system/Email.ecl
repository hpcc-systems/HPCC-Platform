/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
############################################################################## */

import lib_fileservices;

RETURN MODULE

/*
 * Sends an email message using a mail server.
 * 
 * @param sendto        A comma-delimited list of the addresses of the intended recipients.
 * @param subject       The subject line.
 * @param body          The text of the email to send.
 * @param server        The name of the mail server. Defaults to GETENV(SMTPserver).
 * @param port          The port number on the server to connect to. Defaults to GETENV(SMTPport)
 * @param sender        The sender of the email. Defaults to GETENV(emailSenderAddress)
 */

EXPORT SendEmail(varstring to, varstring subject, varstring body, varstring mailServer=GETENV('SMTPserver'), unsigned4 port=(unsigned4) GETENV('SMTPport', '25'), varstring sender=GETENV('emailSenderAddress')) :=
    lib_fileservices.FileServices.SendEmail(to, subject, body, mailServer, port, sender);

/*
 * Sends an email message with a text attachment using a mail server.
 * 
 * @param sendto        A comma-delimited list of the addresses of the intended recipients.
 * @param subject       The subject line.
 * @param body          The text of the email to send.
 * @param attachment    The text of the attachment to send.  Must be a valid string with no embedded nulls.
 * @param server        The name of the mail server. Defaults to GETENV(SMTPserver).
 * @param port          The port number on the server to connect to. Defaults to GETENV(SMTPport)
 * @param sender        The sender of the email. Defaults to GETENV(emailSenderAddress)
 */

EXPORT SendEmailAttachText(varstring to, varstring subject, varstring body, varstring attachment, varstring mimeType, varstring attachmentName, varstring mailServer=GETENV('SMTPserver'), unsigned4 port=(unsigned4) GETENV('SMTPport', '25'), varstring sender=GETENV('emailSenderAddress')) :=
    lib_fileservices.FileServices.SendEmailAttachText(to, subject, body, attachment, mimeType, attachmentName, mailServer, port, sender);

/*
 * Sends an email message with an arbitrary attachment using a mail server.
 * 
 * @param sendto        A comma-delimited list of the addresses of the intended recipients.
 * @param subject       The subject line.
 * @param body          The text of the email to send.
 * @param attachment    The attachment to send.
 * @param mimeType      The mem type of the attachment.  E.g. 'text/plain; charset=ISO-8859-3'
 * @param attachmentName The name of the attachement - often a filename.
 * @param server        The name of the mail server. Defaults to GETENV(SMTPserver).
 * @param port          The port number on the server to connect to. Defaults to GETENV(SMTPport)
 * @param sender        The sender of the email. Defaults to GETENV(emailSenderAddress)
 */

EXPORT SendEmailAttachData(varstring to, varstring subject, varstring body, data attachment, varstring mimeType, varstring attachmentName, varstring mailServer=GETENV('SMTPserver'), unsigned4 port=(unsigned4) GETENV('SMTPport', '25'), varstring sender=GETENV('emailSenderAddress')) :=
    lib_fileservices.FileServices.SendEmailAttachData(to, subject, body, attachment, mimeType, attachmentName, mailServer, port, sender);

END;
