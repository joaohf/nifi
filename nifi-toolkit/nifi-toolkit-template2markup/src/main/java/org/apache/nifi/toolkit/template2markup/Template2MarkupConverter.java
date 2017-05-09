package org.apache.nifi.toolkit.template2markup;

import org.apache.nifi.web.api.dto.TemplateDTO;
import org.apache.nifi.web.api.entity.TemplateEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

public class Template2MarkupConverter extends Closeable {

    private static final Logger logger = LoggerFactory.getLogger(Template2MarkupConverter.class);

    public void convert(final InputStream in) {
        // unmarshal the template
        final TemplateDTO template;
        try {
            JAXBContext context = JAXBContext.newInstance(TemplateDTO.class);
            Unmarshaller unmarshaller = context.createUnmarshaller();
            JAXBElement<TemplateDTO> templateElement = unmarshaller.unmarshal(new StreamSource(in), TemplateDTO.class);
            template = templateElement.getValue();
        } catch (JAXBException jaxbe) {
            logger.warn("An error occurred while parsing a template.", jaxbe);
            //String responseXml = String.format("<errorResponse status=\"%s\" statusText=\"The specified template is not in a valid format.\"/>", Response.Status.BAD_REQUEST.getStatusCode());
            //return Response.status(Response.Status.OK).entity(responseXml).type("application/xml").build();
            return;
        } catch (IllegalArgumentException iae) {
            logger.warn("Unable to import template.", iae);
            //String responseXml = String.format("<errorResponse status=\"%s\" statusText=\"%s\"/>", Response.Status.BAD_REQUEST.getStatusCode(), iae.getMessage());
            //return Response.status(Response.Status.OK).entity(responseXml).type("application/xml").build();
            return;
        } catch (Exception e) {
            logger.warn("An error occurred while importing a template.", e);
            //String responseXml = String.format("<errorResponse status=\"%s\" statusText=\"Unable to import the specified template: %s\"/>",
            //        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.getMessage());
            //return Response.status(Response.Status.OK).entity(responseXml).type("application/xml").build();
            return;
        }

        // build the response entity
        TemplateEntity entity = new TemplateEntity();
        entity.setTemplate(template);

        // verify the template was specified
        if (entity == null || entity.getTemplate() == null || entity.getTemplate().getSnippet() == null) {
            throw new IllegalArgumentException("Template details must be specified.");
        }

        return;
    }

    public void close() throws IOException {

    }

    /**
     * The Builder is the mechanism by which all configuration is passed to the Template2MarkupConverter
     */
    public static class Builder {

        public Template2MarkupConverter build() {
            if (urls == null) {
                throw new IllegalStateException("Must specify URL to build Site-to-Site client");
            }

            if (portName == null && portIdentifier == null) {
                throw new IllegalStateException("Must specify either Port Name or Port Identifier to build Site-to-Site client");
            }

            switch (transportProtocol){
                case RAW:
                    return new SocketClient(buildConfig());
                case HTTP:
                    return new HttpClient(buildConfig());
                default:
                    throw new IllegalStateException("Transport protocol '" + transportProtocol + "' is not supported.");
            }
        }

    }

}