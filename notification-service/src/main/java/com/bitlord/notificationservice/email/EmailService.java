package com.bitlord.notificationservice.email;

import jakarta.mail.internet.MimeMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

import java.util.Map;


/**
 * Service for sending HTML emails using Thymeleaf templates.
 */
@Service
public class EmailService {

    // Logger for tracking email send success and failure events
    private static final Logger log = LoggerFactory.getLogger(EmailService.class);

    // Spring's mail sender used to construct and dispatch email messages
    private final JavaMailSender mailSender;

    // Thymeleaf engine used to render HTML email templates with dynamic data
    private final TemplateEngine templateEngine;

    // Constructor injection — wires in the mail sender and template engine dependencies
    public EmailService(JavaMailSender mailSender, TemplateEngine templateEngine) {
        this.mailSender = mailSender;
        this.templateEngine = templateEngine;
    }

    /**
     * Sends an HTML email using a Thymeleaf template.
     */
    public void sendHtmlEmail(String to, String subject, String templateName, Map<String, Object> templateModel) {
        try {
            // Create a Thymeleaf context and load the dynamic data (e.g. order details, user name)
            Context thymeleafContext = new Context();
            thymeleafContext.setVariables(templateModel);

            // Process the Thymeleaf template file located under resources/templates/email/
            // and render it into a final HTML string
            String htmlBody = templateEngine.process("email/" + templateName, thymeleafContext);

            // Create a new MIME email message (supports HTML, attachments, encoding)
            MimeMessage message = mailSender.createMimeMessage();

            // MimeMessageHelper simplifies setting email fields; true = multipart, UTF-8 = character encoding
            MimeMessageHelper helper = new MimeMessageHelper(message, true, "UTF-8");

            helper.setTo(to);           // Set the recipient email address
            helper.setSubject(subject); // Set the email subject line
            helper.setText(htmlBody, true); // Set the email body as HTML (true = isHtml)
            helper.setFrom("noreply@bitlord-computer-parts.com"); // Set the sender address

            // Dispatch the constructed email via the mail server
            mailSender.send(message);
            log.info("HTML email sent successfully to {} using template {}", to, templateName);
        } catch (Exception e) {
            // Log the failure without throwing — prevents email errors from crashing the service
            log.error("Failed to send HTML email to {}", to, e);
        }
    }
}