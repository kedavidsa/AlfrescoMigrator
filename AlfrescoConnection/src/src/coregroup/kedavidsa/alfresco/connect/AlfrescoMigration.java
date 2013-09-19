package src.coregroup.kedavidsa.alfresco.connect;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Scanner;

import org.apache.chemistry.opencmis.client.api.CmisObject;
import org.apache.chemistry.opencmis.client.api.Document;
import org.apache.chemistry.opencmis.client.api.Folder;
import org.apache.chemistry.opencmis.client.api.Repository;
import org.apache.chemistry.opencmis.client.api.Session;
import org.apache.chemistry.opencmis.client.api.SessionFactory;
import org.apache.chemistry.opencmis.client.runtime.SessionFactoryImpl;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.SessionParameter;
import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.enums.BindingType;
import org.apache.chemistry.opencmis.commons.enums.VersioningState;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.ContentStreamImpl;



public class AlfrescoMigration  {


	
	public static final String ROOT_SPACE="";

	
	private Session session;

	public AlfrescoMigration(String rutaCMIS, String user, String password, String rutaBodega, String logbueno, String logmalo) 
	{

		SessionFactory sessionFactory = SessionFactoryImpl.newInstance();
		Map<String, String> parameter = new HashMap<String, String>();

		// user credentials
		parameter.put(SessionParameter.USER, user);
		parameter.put(SessionParameter.PASSWORD, password);

		// connection settings
		parameter.put(SessionParameter.ATOMPUB_URL, rutaCMIS);
		parameter.put(SessionParameter.BINDING_TYPE, BindingType.ATOMPUB.value());
		//		parameter.put(SessionParameter.REPOSITORY_ID, "myRepository");
		//
		//		// create session
		//		Session session = factory.createSession(parameter);

		//		parameter.put(SessionParameter.REPOSITORY_ID, "myRepository");
		//		// create session
		//		Session session = sessionFactory.createSession(parameter);

		Repository repository = sessionFactory.getRepositories(parameter).get(0);
		System.out.println(repository.getName());
		session = repository.createSession();

		File inicial= new File(rutaBodega);

		File[] lista=inicial.listFiles();
		System.out.println("Fila inicial:" +rutaBodega+" Total carpetas:"+lista.length);
		
		
		 DateFormat df = new SimpleDateFormat("yyyy-MM-dd_kk-mm-ss", Locale.ENGLISH);
		 Date d=new Date();
		try {
			PrintWriter esb=new PrintWriter(new File(logbueno+"/migracionAlfrescoJava"+df.format(d)+".log"));
			PrintWriter ese=new PrintWriter(new File(logmalo+"/migracionAlfrescoJavaError"+df.format(d)+".log"));
		
		for (int i = 0; i < lista.length; i++) {

			File temp=lista[i];

			if(temp.isDirectory())
			{
				System.out.println("Directorio: "+lista[i].getAbsolutePath());
				try
				{
					subirArchivos(temp,esb,ese);
				}
				catch (Exception e)
				{
					System.err.println("Error subiendo archivo-"+lista[i].getAbsolutePath()+" "+e.getMessage());
					//e.printStackTrace();
				}
			}
			else if(temp.isFile())
			{
				try
				{
					
						System.out.println("Archivo: "+lista[i].getAbsolutePath().replace(lista[i].getName()+" Tamano:"+temp.length(), ""));
						String[] rutaAlfresco=lista[i].getAbsolutePath().split("bodega");
						//System.out.println(rutaAlfresco[1].split("[\\\\]")[0]);
						String rutaAlfres=rutaAlfresco[1].replace("\\", "/");
						rutaAlfres=rutaAlfres.replace(lista[i].getName(),"");
						enviarDocumento(temp.getName(),lista[i].getAbsolutePath(),rutaAlfres );
						esb.println("Subido archivo: "+lista[i].getAbsolutePath().replace(lista[i].getName()+" Tamano:"+temp.length(), ""));
						
				}
				catch (Exception e)
				{
					ese.println("Error subiendo archivo-"+lista[i].getAbsolutePath()+" "+e.getMessage());
					
					System.err.println("Error subiendo archivo-"+lista[i].getAbsolutePath()+" "+e.getMessage());
					//e.printStackTrace();
				}


			}
		}
		
		
		esb.close();
		ese.close();
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			System.err.println("No se puede crear el log");
		}
	}

	public void subirArchivos(File ruta, PrintWriter esb, PrintWriter ese) throws Exception
	{

		File[] lista=ruta.listFiles();
		//System.out.println("Fila inicial:" +FILE_START+" Total carpetas:"+lista.length);

		for (int i = 0; i < lista.length; i++) {

			File temp=lista[i];

			if(temp.isDirectory())
			{

				System.out.println("Directorio: "+lista[i].getAbsolutePath());
				try
				{
					subirArchivos(temp,esb,ese);
				}
				catch (Exception e)
				{
					System.err.println("Error subiendo folder-"+lista[i].getAbsolutePath());
					//e.printStackTrace();
				}
			}
			else if(temp.isFile())
			{
				try
				{
//					if(temp.length()<=150000000)
//					{
						System.out.println("Archivo: "+lista[i].getAbsolutePath().replace(lista[i].getName()+" Tamano:"+temp.length(), ""));
						String[] rutaAlfresco=lista[i].getAbsolutePath().split("bodega");
						//System.out.println(rutaAlfresco[1].split("[\\\\]")[0]);
						String rutaAlfres=rutaAlfresco[1].replace("\\", "/");
						rutaAlfres=rutaAlfres.replace(lista[i].getName(),"");
						
						enviarDocumento(temp.getName(),lista[i].getAbsolutePath(),rutaAlfres );
						esb.println("Archivo: "+lista[i].getAbsolutePath().replace(lista[i].getName()+" Tamano:"+temp.length(), ""));
						
//					}else
//					{
//						System.err.println("Error de archivo por tamano: "+lista[i].getAbsolutePath().replace(lista[i].getName()+" Tamano:"+temp.length(), ""));
//						
//					}
				}
				catch (Exception e)
				{
					ese.println("Error subiendo archivo-"+lista[i].getAbsolutePath()+" "+e.getMessage());
					
					System.err.println("Error subiendo archivo-"+lista[i].getAbsolutePath()+" "+e.getMessage());
					
				}
			}

		}
	}

	public void enviarDocumento( String nombre,
			String pathArchivoreal,String pathAlfr) throws Exception{
		// TODO Auto-generated method stub




		//List<Repository> repositories = factory.getRepositories(parameter);
		//Session session = repositories.get(0).createSession();

		String pathAlfresco = ROOT_SPACE+pathAlfr;
		//System.out.println("Subido en:"+pathAlfresco);

		CmisObject object =null;
		try
		{
			object = session.getObjectByPath(pathAlfresco);
		}
		catch(Exception e1)
		{
			//e1.printStackTrace();
		}
		if(object!=null&&object instanceof Folder)
		{
			try {


				String idDocument=getDocumentIdByPath(pathAlfresco+nombre);


				if(idDocument==null||idDocument.equals(""))
				{

					try
					{
						// content
						//byte[] content = "Hello World!".getBytes();
						
							// properties 
							// (minimal set: name and object type id)
							Map<String, Object> properties2 = new HashMap<String, Object>();
							properties2.put(PropertyIds.OBJECT_TYPE_ID, "cmis:document");
							properties2.put(PropertyIds.NAME, nombre);

							// content
							byte[] content = "No se ha subido archivo a alfresco".getBytes();
							
							
							InputStream stream = new ByteArrayInputStream(content);
							 ContentStream contentStream =session.getObjectFactory().createContentStream(nombre, 0, "text/plain",  null);

							 
							

								
							// create a major version

							Document newDoc = ((Folder)object).createDocument(properties2, contentStream, VersioningState.MAJOR);
//							File input=new File(pathArchivoreal);
//							FileInputStream fi=new FileInputStream(input);
//							ContentStream contentStream2=session.getObjectFactory().createContentStream(nombre, input.length(), "text/plain",fi  );
							
							
							 Path path22 = Paths.get(pathArchivoreal);
							byte[] data2 = Files.readAllBytes(path22);
							InputStream stream2 = new ByteArrayInputStream(data2);
							
							
							ContentStream contentStream2 = session.getObjectFactory().createContentStream(nombre, data2.length, "text/plain", stream2);
							

							 newDoc.setContentStream(contentStream2,true,true);
							System.out.println("Documento creado "+pathArchivoreal);
					}
					catch (OutOfMemoryError error)
					{
						//error.printStackTrace();
						System.out.println("El archivo no cabe en memoria "+pathArchivoreal);
					}
				}
				else
				{
					System.err.println("Ya existe archivo: "+pathArchivoreal );
					
					
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
				try
				{
					
					// content
					//byte[] content = "Hello World!".getBytes();
					
						// properties 
						// (minimal set: name and object type id)
						Map<String, Object> properties2 = new HashMap<String, Object>();
						properties2.put(PropertyIds.OBJECT_TYPE_ID, "cmis:document");
						properties2.put(PropertyIds.NAME, nombre);

						// content
						byte[] content = "Hello World!".getBytes();
						File input=new File(pathArchivoreal);
						
						InputStream stream = new ByteArrayInputStream(content);
						 ContentStream contentStream =session.getObjectFactory().createContentStream(nombre, 0, "text/plain",  null);

							
						// create a major version

						Document newDoc = ((Folder)object).createDocument(properties2, contentStream, VersioningState.MAJOR);
						
						
						 ContentStream contentStream2=session.getObjectFactory().createContentStream(nombre, input.length(), "text/plain",  new FileInputStream(input));

						 newDoc.setContentStream(contentStream2,true,true);
						System.out.println("Documento creado "+pathArchivoreal);
					
					


				} catch (OutOfMemoryError error)
				{
					//e.printStackTrace();
					System.out.println("El archivo no cabe en memoria "+pathArchivoreal);
				}catch (Exception e2) {
					// TODO Auto-generated catch block
					//e.printStackTrace();
					throw new Exception("Error al leer archivo para enviar."+ e2.getMessage());
				}



			}
		}else
		{
			throw new Exception("Carpetas no creadas en alfresco");
		}




	}




	public String getDocumentIdByPath(String path) throws Exception
	{
		//ConstantesOrfeoScan orfeo=ConstantesOrfeoScan.getInstance();
//
//		SessionFactory sessionFactory = SessionFactoryImpl.newInstance();
//		Map<String, String> parameter = new HashMap<String, String>();
//
//		// user credentials
//		parameter.put(SessionParameter.USER, USUARIO);
//		parameter.put(SessionParameter.PASSWORD, PASSWORD);
//
//		// connection settings
//		parameter.put(SessionParameter.ATOMPUB_URL, RUTA);
//		parameter.put(SessionParameter.BINDING_TYPE, BindingType.ATOMPUB.value());
//		//		parameter.put(SessionParameter.REPOSITORY_ID, "myRepository");
//		//
//		//		// create session
//		//		Session session = factory.createSession(parameter);
//
//		//		parameter.put(SessionParameter.REPOSITORY_ID, "myRepository");
//		//		// create session
//		//		Session session = sessionFactory.createSession(parameter);
//
//		Repository repository = sessionFactory.getRepositories(parameter).get(0);
//		System.out.println(repository.getName());
//		Session session = repository.createSession();
		String id =null;
		try
		{
		CmisObject object = session.getObjectByPath(path);
		id = object.getId();
		}
		catch (Exception e)
		{
			//e.printStackTrace();
		}

		return id;


	}
	/**
	 * Los argumentos reciben la ruta del servidor CMIS, el usuario, el password y la ruta de la carpeta que se quiere migrar(debe incluir "/bodega" y deben estar las carpetas creadas en alfresco)
	 * @param args
	 */
	public static final void main(String [ ]  args )
	{
		System.out.println("Bienvenido al asistente de migracion Java Bodega to Alfresco");
		System.out.println("Los argumentos que se reciben son: ");
		System.out.println("-La ruta del servidor CMIS"+args[0]);
		System.out.println("-El usuario:"+args[1]);
		System.out.println("-El password:"+args[2]);
		System.out.println("-La ruta de la carpeta que se quiere migrar,debe incluir bodega:"+args[3]);
		System.out.println("Ruta de logs para archivos cargados:"+args[4]);
		System.out.println("Ruta de logs para archivos no cargados:"+args[5]);
		
		Scanner entrada = new Scanner(System.in);
		System.out.print("Desea continuar S/N: ");
		
		String num = entrada.nextLine();
		if(num.toLowerCase().equals("s"))
		{
			System.out.println("Comenzando proceso de migracion.....");
		new AlfrescoMigration(args[0],args[1],args[2],args[3],args[4],args[5]);
		}
		else
		{
			Scanner entrada2 = new Scanner(System.in);
			System.out.println("Proceso cancelado..... presione tecla para continuar");
			
			
			String num2 = entrada2.nextLine();
			if(num2.toLowerCase().equals(""))
			{
				System.out.println("Cerrando");
			}
			
		}
		
		Scanner entrada2 = new Scanner(System.in);
		System.out.println("Proceso terminado..... presione tecla para continuar");
		
		
		String num2 = entrada2.nextLine();
		if(num2.toLowerCase().equals(""))
		{
			System.out.println("Cerrando");
		}
	}

}
