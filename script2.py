import pyodbc
import pandas as pd
import csv


class DBOperation(object):

	def __init__(self, **kwargs):
		self.conn = self._connect()

	def _connect(self):
		conn = None

		try:
			conn = pyodbc.connect(
				driver=kwargs.get('driver', '{SQL Server}'),
				server=kwargs.get('server', ''),
				database=kwargs.get('database', ''),
				trusted_connection='yes',
				uid=kwargs.get('uid', ''),
				pwd=kwargs.get('password', '')
				)
		except pyodbc.Error as ex:
        	sqlstate = ex.args[1]
        	print(sqlstate)
        	
        return conn

    def get_connection(self):
    	return self.conn

	def get_cursor(self):
		if not self.conn:
			raise Exception('Connection not established yet')

		return self.conn.cursor()

	def fetch_rows(self, query):
		cursor = None
		
		try:
			cursor = self.get_cursor()
		except Exception as ex:
			raise ex

		cursor.execute(query)
		return cursor.fetchall()

	def close_connection(self):
		cursor = None
		
		try:
			cursor = self.get_cursor()
		except Exception as ex:
			raise ex

		cursor.close()
		del cursor
		self.conn.close()


if __name__ == '__main__':
	query = """
		USE DEMO_WebTrack;

		SELECT DISTINCT NpiNum AS NPI, 
			Fname AS [First Name],
			LName AS [Last Name], 
			pet.ProjectEntityTypeName AS Degree, 
			'FL' AS [Practice State],
			'INITIAL' AS [Initial/Recredential],
			caqhNum AS [CAQH ID],
			CASE WHEN pe.IsGendermale=1 THEN 'Male' ELSE 'Female' END AS Gender,
			DEA.IdValue AS [DEA Number],
			DateOfBirth AS [Date of Birth], 
			LN.IdValue AS [License Number],
			SocialSecurityNum AS SSN,
			MS.IdValue AS School,
			GY.IdValue AS [Graduation Year],
			CASE WHEN es.IsBoardCertified=1 THEN 'Board Certified' ELSE 'Not Certified' END AS [Board Certified],
			nbc.Idvalue AS [Board Name],
			pe.MName AS [Middle Name],
			TC.IdValue AS [Taxonomy Code],
			splty.IdValue AS Specialty,
			lng.languageName AS [Additional language],
			pe.Email,
			addr.ContactFName AS [Cred Contact First Name],
			addr.ContactLName AS [Cred Contact Last Name],
			addr.Email AS [Cred Contact Email],
			addr.Phone AS [Phone Number],
			'' AS [Fax], '' AS [Delegated Roster Provider]
			FROM ProjectEntities AS pe
			JOIN EntityTypes et ON pe.EntityTypeID=et.EntityTypeID 
			JOIN ProjectEntityTypes pet ON pe.ProjectEntityTypeId=pet.ProjectEntityTypeId
			JOIN EntityContracts ec ON ec.ProjectEntityId = pe.ProjectEntityId
			JOIN Contracts c ON ec.ContractId=c.ContractID
			LEFT JOIN (select ProjectEntityId, idvalue FROM EntityIdValues eiv JOIN IdTypes it ON eiv.IdTypeId = it.IDTypeID AND IdTypeName= 'DEA Number') AS DEA ON dea.ProjectEntityId=pe.ProjectEntityId
			LEFT JOIN (select ProjectEntityId, idvalue FROM EntityIdValues eiv1 JOIN IdTypes it1 ON eiv1.IdTypeId = it1.IDTypeID AND IdTypeName= 'License') AS LN ON LN.ProjectEntityId=pe.ProjectEntityId
			LEFT JOIN (select ProjectEntityId, idvalue FROM EntityIdValues eiv2 JOIN IdTypes it2 ON eiv2.IdTypeId = it2.IDTypeID AND IdTypeName= 'Medical School') AS MS ON MS.ProjectEntityId=pe.ProjectEntityId
			LEFT JOIN (select ProjectEntityId, idvalue FROM EntityIdValues eiv3 JOIN IdTypes it3 ON eiv3.IdTypeId = it3.IDTypeID AND IdTypeName= 'Graduation Year') AS GY ON GY.ProjectEntityId=pe.ProjectEntityId
			JOIN entityspecialties AS es ON pe.projectentityID=es.projectentityID
			LEFT JOIN (select ProjectEntityId, IdValue FROM EntityIdValues eiv6 JOIN IdTypes it6 ON eiv6.IdTypeId=it6.IDTypeID AND IdTypeName='Name of Board Certification') nbc ON nbc.ProjectEntityId=pe.ProjectEntityId
			LEFT JOIN (select ProjectEntityId, idvalue FROM EntityIdValues eiv4 JOIN IdTypes it4 ON eiv4.IdTypeId = it4.IDTypeID AND IdTypeName= 'Taxonomy Code') AS TC ON TC.ProjectEntityId=pe.ProjectEntityId
			LEFT JOIN (select ProjectEntityId, idvalue FROM EntityIdValues eiv5 JOIN IdTypes it5 ON eiv5.IdTypeId = it5.IDTypeID AND IdTypeName= 'Speciality') AS splty ON splty.ProjectEntityId=pe.ProjectEntityId
			LEFT JOIN (select projectEntityId, LanguageName FROM EntityLanguages el JOIN Languages ln ON el.LanguageId=ln.LanguageID) AS lng ON lng.ProjectEntityId=pe.ProjectEntityId 
			LEFT JOIN (select projectEntityId, ContactFName, ContactLName,Email, ac.Phone FROM Addresses a JOIN AddressContacts ac ON a.AddressId=ac.AddressId WHERE ac.ContactTitle LIKE 'CRED%') AS addr ON addr.ProjectEntityId=[dbo].[fnGetTopParent](pe.ProjectEntityId) 
		WHERE (CaqhNum IS NOT NULL AND pe.CaqhNum <> '') AND (NpiNum IS NOT NULL AND pe.NpiNum <> '') AND (et.IsPerson=1) AND (pe.EducationDegree IS NOT NULL) AND ec.AcceptDate IS NOT NULL
		"""
	
	conn = DBOperation(
		driver="{SQL Server}",
		server="ZWALA-PC\\SQLEXPRESS",
		database="AdventureworksDW2016CTP3",
		uid="ZWALA-PC",
		password="meenadevi23"
		)

	cursor = conn.get_cursor()
	cursor.execute(query)

	columns = [desc[0] for desc in cursor.description]
	rows = cursor.fetchall()
	# df = pd.read_sql_query(query, conn.get_connection())
	df = pd.DataFrame(list(rows), columns=columns)
	writer = pd.ExcelWriter(r'C:\Users\zwala\Desktop\testpython\data.xlsx')
	df.to_excel(writer, sheet_name='original')
	writer.save()

	with open(r'C:\Users\zwala\Desktop\testpython\data.csv', 'w', newline= '') as f:
		_writer = csv.writer(f, delimiter=', ')
		_writer.writerows(rows)

	conn.close_connection()
	