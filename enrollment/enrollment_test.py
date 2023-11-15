import redis
import boto3
import logging

from botocore.exceptions import ClientError
from enrollment_schemas import Class, Enroll, Dropped, User_info
from enrollment_dynamo import Enrollment, PartiQL
from pprint import pprint

# turn debug print statements on or off
DEBUG = False

# Configure the logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Connect to Redis
r = redis.Redis(db=1)
#r = redis.Redis(host='localhost', port=6379, db=1)  # Update connection details as needed

# Connect to DynamoDB
dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:5500')
table_prefix = "enrollment_"

# Lists of dummy names
surnames_list=['Smith','Johnson','Williams','Brown','Jones','Miller','Davis','Garcia','Rodriguez','Wilson','Martinez','Anderson','Taylor','Thomas','Hernandez','Moore','Martin','Jackson','Thompson','White','Lopez','Lee','Gonzalez','Harris','Clark','Lewis','Robinson','Walker','Perez','Hall','Young','Allen','Sanchez','Wright','King','Scott','Green','Baker','Adams','Nelson','Hill','Ramirez','Campbell','Mitchell','Roberts','Carter','Phillips','Evans','Turner','Torres','Parker','Collins','Edwards','Stewart','Flores','Morris','Nguyen','Murphy','Rivera','Cook','Rogers','Morgan','Peterson','Cooper','Reed','Bailey','Bell','Gomez','Kelly','Howard','Ward','Cox','Diaz','Richardson','Wood','Watson','Brooks','Bennett','Gray','James','Reyes','Cruz','Hughes','Price','Myers','Long','Foster','Sanders','Ross','Morales','Powell','Sullivan','Russell','Ortiz','Jenkins','Gutierrez','Perry','Butler','Barnes','Fisher','Henderson','Coleman','Simmons','Patterson','Jordan','Reynolds','Hamilton','Graham','Kim','Gonzales','Alexander','Ramos','Wallace','Griffin','West','Cole','Hayes','Chavez','Gibson','Bryant','Ellis','Stevens','Murray','Ford','Marshall','Owens','Mcdonald','Harrison','Ruiz','Kennedy','Wells','Alvarez','Woods','Mendoza','Castillo','Olson','Webb','Washington','Tucker','Freeman','Burns','Henry','Vasquez','Snyder','Simpson','Crawford','Jimenez','Porter','Mason','Shaw','Gordon','Wagner','Hunter','Romero','Hicks','Dixon','Hunt','Palmer','Robertson','Black','Holmes','Stone','Meyer','Boyd','Mills','Warren','Fox','Rose','Rice','Moreno','Schmidt','Patel','Ferguson','Nichols','Herrera','Medina','Ryan','Fernandez','Weaver','Daniels','Stephens','Gardner','Payne','Kelley','Dunn','Pierce','Arnold','Tran','Spencer','Peters','Hawkins','Grant','Hansen','Castro','Hoffman','Hart','Elliott','Cunningham','Knight','Bradley','Carroll','Hudson','Duncan','Armstrong','Berry','Andrews','Johnston','Ray','Lane','Riley','Carpenter','Perkins','Aguilar','Silva','Richards','Willis','Matthews','Chapman','Lawrence','Garza','Vargas','Watkins','Wheeler','Larson','Carlson','Harper','George','Greene','Burke','Guzman','Morrison','Munoz','Jacobs','Obrien','Lawson','Franklin','Lynch','Bishop','Carr','Salazar','Austin','Mendez','Gilbert','Jensen','Williamson','Montgomery','Harvey','Oliver','Howell','Dean','Hanson','Weber','Garrett','Sims','Burton','Fuller','Soto','Mccoy','Welch','Chen','Schultz','Walters','Reid','Fields','Walsh','Little','Fowler','Bowman','Davidson','May','Day','Schneider','Newman','Brewer','Lucas','Holland','Wong','Banks','Santos','Curtis','Pearson','Delgado','Valdez','Pena','Rios','Douglas','Sandoval','Barrett','Hopkins','Keller','Guerrero','Stanley','Bates','Alvarado','Beck','Ortega','Wade','Estrada','Contreras','Barnett','Caldwell','Santiago','Lambert','Powers','Chambers','Nunez','Craig','Leonard','Lowe','Rhodes','Byrd','Gregory','Shelton','Frazier','Becker','Maldonado','Fleming','Vega','Sutton','Cohen','Jennings','Parks','Mcdaniel','Watts','Barker','Norris','Vaughn','Vazquez','Holt','Schwartz','Steele','Benson','Neal','Dominguez','Horton','Terry','Wolfe','Hale','Lyons','Graves','Haynes','Miles','Park','Warner','Padilla','Bush','Thornton','Mccarthy','Mann','Zimmerman','Erickson','Fletcher','Mckinney','Page','Dawson','Joseph','Marquez','Reeves','Klein','Espinoza','Baldwin','Moran','Love','Robbins','Higgins','Ball','Cortez','Le','Griffith','Bowen','Sharp','Cummings','Ramsey','Hardy','Swanson','Barber','Acosta','Luna','Chandler','Blair','Daniel','Cross','Simon','Dennis','Oconnor','Quinn','Gross','Navarro','Moss','Fitzgerald','Doyle','Mclaughlin','Rojas','Rodgers','Stevenson','Singh','Yang','Figueroa','Harmon','Newton','Paul','Manning','Garner','Mcgee','Reese','Francis','Burgess','Adkins','Goodman','Curry','Brady','Christensen','Potter','Walton','Goodwin','Mullins','Molina','Webster','Fischer','Campos','Avila','Sherman','Todd','Chang','Blake','Malone','Wolf','Hodges','Juarez','Gill','Farmer','Hines','Gallagher','Duran','Hubbard','Cannon','Miranda','Wang','Saunders','Tate','Mack','Hammond','Carrillo','Townsend','Wise','Ingram','Barton','Mejia','Ayala','Schroeder','Hampton','Rowe','Parsons','Frank','Waters','Strickland','Osborne','Maxwell','Chan','Deleon','Norman','Harrington','Casey','Patton','Logan','Bowers','Mueller','Glover','Floyd','Hartman','Buchanan','Cobb','French','Kramer','Mccormick','Clarke','Tyler','Gibbs','Moody','Conner','Sparks','Mcguire','Leon','Bauer','Norton','Pope','Flynn','Hogan','Robles','Salinas','Yates','Lindsey','Lloyd','Marsh','Mcbride','Owen','Solis','Pham','Lang','Pratt','Lara','Brock','Ballard','Trujillo','Shaffer','Drake','Roman','Aguirre','Morton','Stokes','Lamb','Pacheco','Patrick','Cochran','Shepherd','Cain','Burnett','Hess','Li','Cervantes','Olsen','Briggs','Ochoa','Cabrera','Velasquez','Montoya','Roth','Meyers','Cardenas','Fuentes','Weiss','Wilkins','Hoover','Nicholson','Underwood','Short','Carson','Morrow','Colon','Holloway','Summers','Bryan','Petersen','Mckenzie','Serrano','Wilcox','Carey','Clayton','Poole','Calderon','Gallegos','Greer','Rivas','Guerra','Decker','Collier','Wall','Whitaker','Bass','Flowers','Davenport','Conley','Houston','Huff','Copeland','Hood','Monroe','Massey','Roberson','Combs','Franco','Larsen','Pittman','Randall','Skinner','Wilkinson','Kirby','Cameron','Bridges','Anthony','Richard','Kirk','Bruce','Singleton','Mathis','Bradford','Boone','Abbott','Charles','Allison','Sweeney','Atkinson','Horn','Jefferson','Rosales','York','Christian','Phelps','Farrell','Castaneda','Nash','Dickerson','Bond','Wyatt','Foley','Chase','Gates','Vincent','Mathews','Hodge','Garrison','Trevino','Villarreal','Heath','Dalton','Valencia','Callahan','Hensley','Atkins','Huffman','Roy','Boyer','Shields','Lin','Hancock','Grimes','Glenn','Cline','Delacruz','Camacho','Dillon','Parrish','Oneill','Melton','Booth','Kane','Berg','Harrell','Pitts','Savage','Wiggins','Brennan','Salas','Marks','Russo','Sawyer','Baxter','Golden','Hutchinson','Liu','Walter','Mcdowell','Wiley','Rich','Humphrey','Johns','Koch','Suarez','Hobbs','Beard','Gilmore','Ibarra','Keith','Macias','Khan','Andrade','Ware','Stephenson','Henson','Wilkerson','Dyer','Mcclure','Blackwell','Mercado','Tanner','Eaton','Clay','Barron','Beasley','Oneal','Small','Preston','Wu','Zamora','Macdonald','Vance','Snow','Mcclain','Stafford','Orozco','Barry','English','Shannon','Kline','Jacobson','Woodard','Huang','Kemp','Mosley','Prince','Merritt','Hurst','Villanueva','Roach','Nolan','Lam','Yoder','Mccullough','Lester','Santana','Valenzuela','Winters','Barrera','Orr','Leach','Berger','Mckee','Strong','Conway','Stein','Whitehead','Bullock','Escobar','Knox','Meadows','Solomon','Velez','Odonnell','Kerr','Stout','Blankenship','Browning','Kent','Lozano','Bartlett','Pruitt','Buck','Barr','Gaines','Durham','Gentry','Mcintyre','Sloan','Rocha','Melendez','Herman','Sexton','Moon','Hendricks','Rangel','Stark','Lowery','Hardin','Hull','Sellers','Ellison','Calhoun','Gillespie','Mora','Knapp','Mccall','Morse','Dorsey','Weeks','Nielsen','Livingston','Leblanc','Mclean','Bradshaw','Glass','Middleton','Buckley','Schaefer','Frost','Howe','House','Mcintosh','Ho','Pennington','Reilly','Hebert','Mcfarland','Hickman','Noble','Spears','Conrad','Arias','Galvan','Velazquez','Huynh','Frederick','Randolph','Cantu','Fitzpatrick','Mahoney','Peck','Villa','Michael','Donovan','Mcconnell','Walls','Boyle','Mayer','Zuniga','Giles','Pineda','Pace','Hurley','Mays','Mcmillan','Crosby','Ayers','Case','Bentley','Shepard','Everett','Pugh','David','Mcmahon','Dunlap','Bender','Hahn','Harding','Acevedo','Raymond','Blackburn','Duffy','Landry','Dougherty','Bautista','Shah','Potts','Arroyo','Valentine','Meza','Gould','Vaughan','Fry','Rush','Avery','Herring','Dodson','Clements','Sampson','Tapia','Bean','Lynn','Crane','Farley','Cisneros','Benton','Ashley','Mckay','Finley','Best','Blevins','Friedman','Moses','Sosa','Blanchard','Huber','Frye','Krueger','Bernard','Rosario','Rubio','Mullen','Benjamin','Haley','Chung','Moyer','Choi','Horne','Yu','Woodward','Ali','Nixon','Hayden','Rivers','Estes','Mccarty','Richmond','Stuart','Maynard','Brandt','Oconnell','Hanna','Sanford','Sheppard','Church','Burch','Levy','Rasmussen','Coffey','Ponce','Faulkner','Donaldson','Schmitt','Novak','Costa','Montes','Booker','Cordova','Waller','Arellano','Maddox','Mata','Bonilla','Stanton','Compton','Kaufman','Dudley','Mcpherson','Beltran','Dickson','Mccann','Villegas','Proctor','Hester','Cantrell','Daugherty','Cherry','Bray','Davila','Rowland','Madden','Levine','Spence','Good','Irwin','Werner','Krause','Petty','Whitney','Baird','Hooper','Pollard','Zavala','Jarvis','Holden','Haas','Hendrix','Mcgrath','Bird','Lucero','Terrell','Riggs','Joyce','Mercer','Rollins','Galloway','Duke','Odom','Andersen','Downs','Hatfield','Benitez','Archer','Huerta','Travis','Mcneil','Hinton','Zhang','Hays','Mayo','Fritz','Branch','Mooney','Ewing','Ritter','Esparza','Frey','Braun','Gay','Riddle','Haney','Kaiser','Holder','Chaney','Mcknight','Gamble','Vang','Cooley','Carney','Cowan','Forbes','Ferrell','Davies','Barajas','Shea','Osborn','Bright','Cuevas','Bolton','Murillo','Lutz','Duarte','Kidd','Key','Cooke']
male_names_list=['James','John','Robert','Michael','William','David','Richard','Charles','Joseph','Thomas','Christopher','Daniel','Paul','Mark','Donald','George','Kenneth','Steven','Edward','Brian','Ronald','Anthony','Kevin','Jason','Matthew','Gary','Timothy','Jose','Larry','Jeffrey','Frank','Scott','Eric','Stephen','Andrew','Raymond','Gregory','Joshua','Jerry','Dennis','Walter','Patrick','Peter','Harold','Douglas','Henry','Carl','Arthur','Ryan','Roger','Joe','Juan','Jack','Albert','Jonathan','Justin','Terry','Gerald','Keith','Samuel','Willie','Ralph','Lawrence','Nicholas','Roy','Benjamin','Bruce','Brandon','Adam','Harry','Fred','Wayne','Billy','Steve','Louis','Jeremy','Aaron','Randy','Howard','Eugene','Carlos','Russell','Bobby','Victor','Martin','Ernest','Phillip','Todd','Jesse','Craig','Alan','Shawn','Clarence','Sean','Philip','Chris','Johnny','Earl','Jimmy','Antonio','Danny','Bryan','Tony','Luis','Mike','Stanley','Leonard','Nathan','Dale','Manuel','Rodney','Curtis','Norman','Allen','Marvin','Vincent','Glenn','Jeffery','Travis','Jeff','Chad','Jacob','Lee','Melvin','Alfred','Kyle','Francis','Bradley','Jesus','Herbert','Frederick','Ray','Joel','Edwin','Don','Eddie','Ricky','Troy','Randall','Barry','Alexander','Bernard','Mario','Leroy','Francisco','Marcus','Micheal','Theodore','Clifford','Miguel','Oscar','Jay','Jim','Tom','Calvin','Alex','Jon','Ronnie','Bill','Lloyd','Tommy','Leon','Derek','Warren','Darrell','Jerome','Floyd','Leo','Alvin','Tim','Wesley','Gordon','Dean','Greg','Jorge','Dustin','Pedro','Derrick','Dan','Lewis','Zachary','Corey','Herman','Maurice','Vernon','Roberto','Clyde','Glen','Hector','Shane','Ricardo','Sam','Rick','Lester','Brent','Ramon','Charlie','Tyler','Gilbert','Gene','Marc','Reginald','Ruben','Brett','Angel','Nathaniel','Rafael','Leslie','Edgar','Milton','Raul','Ben','Chester','Cecil','Duane','Franklin','Andre','Elmer','Brad','Gabriel','Ron','Mitchell','Roland','Arnold','Harvey','Jared','Adrian','Karl','Cory','Claude','Erik','Darryl','Jamie','Neil','Jessie','Christian','Javier','Fernando','Clinton','Ted','Mathew','Tyrone','Darren','Lonnie','Lance','Cody','Julio','Kelly','Kurt','Allan','Nelson','Guy','Clayton','Hugh','Max','Dwayne','Dwight','Armando','Felix','Jimmie','Everett','Jordan','Ian','Wallace','Ken','Bob','Jaime','Casey','Alfredo','Alberto','Dave','Ivan','Johnnie','Sidney','Byron','Julian','Isaac','Morris','Clifton','Willard','Daryl','Ross','Virgil','Andy','Marshall','Salvador','Perry','Kirk','Sergio','Marion','Tracy','Seth','Kent','Terrance','Rene','Eduardo','Terrence','Enrique','Freddie','Wade']
female_names_list=['Mary','Patricia','Linda','Barbara','Elizabeth','Jennifer','Maria','Susan','Margaret','Dorothy','Lisa','Nancy','Karen','Betty','Helen','Sandra','Donna','Carol','Ruth','Sharon','Michelle','Laura','Sarah','Kimberly','Deborah','Jessica','Shirley','Cynthia','Angela','Melissa','Brenda','Amy','Anna','Rebecca','Virginia','Kathleen','Pamela','Martha','Debra','Amanda','Stephanie','Carolyn','Christine','Marie','Janet','Catherine','Frances','Ann','Joyce','Diane','Alice','Julie','Heather','Teresa','Doris','Gloria','Evelyn','Jean','Cheryl','Mildred','Katherine','Joan','Ashley','Judith','Rose','Janice','Kelly','Nicole','Judy','Christina','Kathy','Theresa','Beverly','Denise','Tammy','Irene','Jane','Lori','Rachel','Marilyn','Andrea','Kathryn','Louise','Sara','Anne','Jacqueline','Wanda','Bonnie','Julia','Ruby','Lois','Tina','Phyllis','Norma','Paula','Diana','Annie','Lillian','Emily','Robin','Peggy','Crystal','Gladys','Rita','Dawn','Connie','Florence','Tracy','Edna','Tiffany','Carmen','Rosa','Cindy','Grace','Wendy','Victoria','Edith','Kim','Sherry','Sylvia','Josephine','Thelma','Shannon','Sheila','Ethel','Ellen','Elaine','Marjorie','Carrie','Charlotte','Monica','Esther','Pauline','Emma','Juanita','Anita','Rhonda','Hazel','Amber','Eva','Debbie','April','Leslie','Clara','Lucille','Jamie','Joanne','Eleanor','Valerie','Danielle','Megan','Alicia','Suzanne','Michele','Gail','Bertha','Darlene','Veronica','Jill','Erin','Geraldine','Lauren','Cathy','Joann','Lorraine','Lynn','Sally','Regina','Erica','Beatrice','Dolores','Bernice','Audrey','Yvonne','Annette','June','Samantha','Marion','Dana','Stacy','Ana','Renee','Ida','Vivian','Roberta','Holly','Brittany','Melanie','Loretta','Yolanda','Jeanette','Laurie','Katie','Kristen','Vanessa','Alma','Sue','Elsie','Beth','Jeanne','Vicki','Carla','Tara','Rosemary','Eileen','Terri','Gertrude','Lucy','Tonya','Ella','Stacey','Wilma','Gina','Kristin','Jessie','Natalie','Agnes','Vera','Willie','Charlene','Bessie','Delores','Melinda','Pearl','Arlene','Maureen','Colleen','Allison','Tamara','Joy','Georgia','Constance','Lillie','Claudia','Jackie','Marcia','Tanya','Nellie','Minnie','Marlene','Heidi','Glenda','Lydia','Viola','Courtney','Marian','Stella','Caroline','Dora','Jo','Vickie','Mattie','Terry','Maxine','Irma','Mabel','Marsha','Myrtle','Lena','Christy','Deanna','Patsy','Hilda','Gwendolyn','Jennie','Nora','Margie','Nina','Cassandra','Leah','Penny','Kay','Priscilla','Naomi','Carole','Brandy','Olga','Billie','Dianne','Tracey','Leona','Jenny','Felicia','Sonia','Miriam','Velma','Becky','Bobbie','Violet','Kristina','Toni','Misty','Mae','Shelly','Daisy','Ramona','Sherri','Erika','Katrina','Claire','Lindsey','Lindsay','Geneva','Guadalupe','Belinda','Margarita','Sheryl','Cora','Faye','Ada','Natasha','Sabrina','Isabel','Marguerite','Hattie','Harriet','Molly','Cecilia','Kristi','Brandi','Blanche','Sandy','Rosie','Joanna','Iris','Eunice','Angie','Inez','Lynda','Madeline','Amelia','Alberta','Genevieve','Monique','Jodi','Janie','Maggie','Kayla','Sonya','Jan','Lee','Kristine','Candace','Fannie','Maryann','Opal','Alison','Yvette','Melody','Luz','Susie','Olivia','Flora','Shelley','Kristy','Mamie','Lula','Lola','Verna','Beulah','Antoinette','Candice','Juana','Jeannette','Pam','Kelli','Hannah','Whitney','Bridget','Karla','Celia','Latoya','Patty','Shelia','Gayle','Della','Vicky','Lynne','Sheri','Marianne','Kara','Jacquelyn','Erma','Blanca','Myra','Leticia','Pat','Krista','Roxanne','Angelica','Johnnie','Robyn','Francis','Adrienne','Rosalie','Alexandra','Brooke','Bethany','Sadie','Bernadette','Traci','Jody','Kendra','Jasmine','Nichole','Rachael','Chelsea','Mable','Ernestine','Muriel','Marcella','Elena','Krystal','Angelina','Nadine','Kari','Estelle','Dianna','Paulette','Lora','Mona','Doreen','Rosemarie','Angel','Desiree','Antonia','Hope','Ginger','Janis','Betsy','Christie','Freda','Mercedes','Meredith','Lynette','Teri','Cristina','Eula','Leigh','Meghan','Sophia','Eloise','Rochelle','Gretchen','Cecelia','Raquel','Henrietta','Alyssa','Jana','Kelley','Gwen','Kerry','Jenna','Tricia','Laverne','Olive','Alexis','Tasha','Silvia','Elvira','Casey','Delia','Sophie','Kate','Patti','Lorena','Kellie','Sonja','Lila','Lana','Darla','May','Mindy','Essie','Mandy','Lorene','Elsa','Josefina','Jeannie','Miranda','Dixie','Lucia','Marta','Faith','Lela','Johanna','Shari','Camille','Tami','Shawna','Elisa','Ebony','Melba','Ora','Nettie','Tabitha','Ollie','Jaime','Winifred','Kristie','Marina','Alisha','Aimee','Rena','Myrna','Marla','Tammie','Latasha','Bonita','Patrice','Ronda','Sherrie','Addie','Francine','Deloris','Stacie','Adriana','Cheri','Shelby','Abigail','Celeste','Jewel','Cara','Adele','Rebekah','Lucinda','Dorthy','Chris','Effie','Trina','Reba','Shawn','Sallie','Aurora','Lenora','Etta','Lottie','Kerri','Trisha','Nikki','Estella','Francisca','Josie','Tracie','Marissa','Karin','Brittney','Janelle','Lourdes','Laurel','Helene','Fern','Elva','Corinne','Kelsey','Ina','Bettie','Elisabeth','Aida','Caitlin','Ingrid','Iva','Eugenia','Christa','Goldie','Cassie','Maude','Jenifer','Therese','Frankie','Dena','Lorna','Janette','Latonya','Candy','Morgan','Consuelo','Tamika','Rosetta','Debora','Cherie','Polly','Dina','Jewell','Fay','Jillian','Dorothea','Nell','Trudy','Esperanza','Patrica','Kimberley','Shanna','Helena','Carolina','Cleo','Stefanie','Rosario','Ola','Janine','Mollie','Lupe','Alisa','Lou','Maribel','Susanne','Bette','Susana','Elise','Cecile','Isabelle','Lesley','Jocelyn','Paige','Joni','Rachelle','Leola','Daphne','Alta','Ester','Petra','Graciela','Imogene','Jolene','Keisha','Lacey','Glenna','Gabriela','Keri','Ursula','Lizzie','Kirsten','Shana','Adeline','Mayra','Jayne','Jaclyn','Gracie','Sondra','Carmela','Marisa','Rosalind','Charity','Tonia','Beatriz','Marisol','Clarice','Jeanine','Sheena','Angeline','Frieda','Lily','Robbie','Shauna','Millie','Claudette','Cathleen','Angelia','Gabrielle','Autumn','Katharine','Summer','Jodie','Staci','Lea','Christi','Jimmie','Justine','Elma','Luella','Margret','Dominique','Socorro','Rene','Martina','Margo','Mavis','Callie','Bobbi','Maritza','Lucile','Leanne','Jeannine','Deana','Aileen','Lorie','Ladonna','Willa','Manuela','Gale','Selma','Dolly','Sybil','Abby','Lara','Dale','Ivy','Dee','Winnie','Marcy','Luisa','Jeri','Magdalena','Ofelia','Meagan','Audra','Matilda','Leila','Cornelia','Bianca','Simone','Bettye','Randi','Virgie','Latisha','Barbra','Georgina','Eliza','Leann','Bridgette','Rhoda','Haley','Adela','Nola','Bernadine','Flossie','Ila','Greta','Ruthie','Nelda','Minerva','Lilly','Terrie','Letha','Hilary','Estela','Valarie','Brianna','Rosalyn','Earline','Catalina','Ava','Mia','Clarissa','Lidia','Corrine','Alexandria','Concepcion','Tia','Sharron','Rae','Dona','Ericka','Jami','Elnora','Chandra','Lenore','Neva','Marylou','Melisa','Tabatha','Serena','Avis','Allie','Sofia','Jeanie','Odessa','Nannie','Harriett','Loraine','Penelope','Milagros','Emilia','Benita','Allyson','Ashlee','Tania','Tommie','Esmeralda','Karina','Eve','Pearlie','Zelma','Malinda','Noreen','Tameka','Saundra','Hillary','Amie','Althea','Rosalinda','Jordan','Lilia','Alana','Gay','Clare','Alejandra','Elinor','Michael','Lorrie','Jerri','Darcy','Earnestine','Carmella','Taylor','Noemi','Marcie','Liza','Annabelle','Louisa','Earlene','Mallory','Carlene','Nita','Selena','Tanisha','Katy','Julianne','John','Lakisha','Edwina','Maricela','Margery','Kenya','Dollie','Roxie','Roslyn','Kathrine','Nanette','Charmaine','Lavonne','Ilene','Kris','Tammi','Suzette','Corine','Kaye','Jerry','Merle','Chrystal','Lina','Deanne','Lilian','Juliana','Aline','Luann','Kasey','Maryanne','Evangeline','Colette','Melva','Lawanda','Yesenia','Nadia','Madge','Kathie','Eddie','Ophelia','Valeria','Nona','Mitzi','Mari','Georgette','Claudine','Fran','Alissa','Roseann','Lakeisha','Susanna','Reva','Deidre','Chasity','Sheree','Carly','James','Elvia','Alyce','Deirdre','Gena','Briana','Araceli','Katelyn','Rosanne','Wendi','Tessa','Berta','Marva','Imelda','Marietta','Marci','Leonor','Arline','Sasha','Madelyn','Janna','Juliette','Deena','Aurelia','Josefa','Augusta','Liliana','Young','Christian','Lessie','Amalia','Savannah','Anastasia','Vilma','Natalia','Rosella','Lynnette','Corina','Alfreda','Leanna','Carey','Amparo','Coleen','Tamra','Aisha','Wilda','Karyn','Cherry','Queen','Maura','Mai','Evangelina','Rosanna','Hallie','Erna','Enid','Mariana','Lacy','Juliet','Jacklyn','Freida','Madeleine','Mara','Hester','Cathryn','Lelia','Casandra','Bridgett','Angelita','Jannie','Dionne','Annmarie','Katina','Beryl','Phoebe','Millicent','Katheryn','Diann','Carissa','Maryellen','Liz','Lauri','Helga','Gilda','Adrian','Rhea','Marquita','Hollie','Tisha','Tamera','Angelique','Francesca','Britney','Kaitlin','Lolita','Florine','Rowena','Reyna','Twila','Fanny','Janell','Ines','Concetta','Bertie','Alba','Brigitte','Alyson','Vonda','Pansy','Elba','Noelle','Letitia','Kitty','Deann','Brandie','Louella','Leta','Felecia','Sharlene','Lesa','Beverley','Robert','Isabella','Herminia','Terra','Celina']

# Create a list of male first/last names
mname = []
last = 0
for male in male_names_list:
    if last >= len(surnames_list):
        last = 0
    mname.append(male + ' ' + surnames_list[last])
    last += 1

# Create a list of female first/last names
fname = []
last = len(surnames_list)-1
for female in female_names_list:
    if last < 0:
        last = len(surnames_list)-1
    fname.append(female + ' ' + surnames_list[last])
    last -= 1

# Combind the lists of names
last = 0
names = []
for male in mname:
    names.append(male)
    names.append(fname[last])
    last += 1

# Create sample data
department = ["CHEM","CPSC","ENGL","MATH","PHYS","HIST","BIOL","GEOL"]

sample_classes = [
    Class(
        id=1,
        name="Web Back-End Engineering",
        course_code="449",
        section_number=1,
        current_enroll=10,
        max_enroll=30,
        department="CPSC",
        instructor_id=501,
        enrolled=[],
        dropped=[11,12,13,14],
    ),
    Class(
        id=2,
        name="Web Back-End Engineering",
        course_code="449",
        section_number=2,
        current_enroll=24,
        max_enroll=30,
        department="CPSC",
        instructor_id=502,
        enrolled=[],
        dropped=[],
    ),
    Class(
        id=3,
        name="Web Front-End Engineering",
        course_code="349",
        section_number=1,
        current_enroll=14,
        max_enroll=30,
        department="CPSC",
        instructor_id=503,
        enrolled=[],
        dropped=[],
    ),
    Class(
        id=4,
        name="Introduction to Computer Science",
        course_code="120",
        section_number=1,
        current_enroll=32,
        max_enroll=30,
        department="CPSC",
        instructor_id=504,
        enrolled=[],
        dropped=[],
    ),
    Class(
        id=5,
        name="Calculus I",
        course_code="150A",
        section_number=1,
        current_enroll=28,
        max_enroll=30,
        department="MATH",
        instructor_id=505,
        enrolled=[],
        dropped=[],
    ),
    Class(
        id=6,
        name="Calculus II",
        course_code="150B",
        section_number=1,
        current_enroll=30,
        max_enroll=30,
        department="MATH",
        instructor_id=506,
        enrolled=[],
        dropped=[],
    ),
    Class(
        id=7,
        name="World History",
        course_code="181",
        section_number=1,
        current_enroll=15,
        max_enroll=30,
        department="HIST",
        instructor_id=507,
        enrolled=[],
        dropped=[],
    ),
    Class(
        id=8,
        name="Anatomy & Physiology",
        course_code="211",
        section_number=1,
        current_enroll=30,
        max_enroll=30,
        department="BIOL",
        instructor_id=508,
        enrolled=[],
        dropped=[],
    ),
    Class(
        id=9,
        name="Earth Science",
        course_code="171",
        section_number=1,
        current_enroll=28,
        max_enroll=30,
        department="GEOL",
        instructor_id=509,
        enrolled=[],
        dropped=[],
    ),
    Class(
        id=10,
        name="Advanced C++",
        course_code="421",
        section_number=1,
        current_enroll=12,
        max_enroll=30,
        department="CPSC",
        instructor_id=510,
        enrolled=[],
        dropped=[],
    ),
    Class(
        id=11,
        name="Python Programming",
        course_code="222",
        section_number=1,
        current_enroll=27,
        max_enroll=30,
        department="CPSC",
        instructor_id=511,
        enrolled=[],
        dropped=[],
    ),
    Class(
        id=12,
        name="Python Programming",
        course_code="222",
        section_number=2,
        current_enroll=45,
        max_enroll=30,
        department="CPSC",
        instructor_id=511,
        enrolled=[],
        dropped=[],
    ),
    Class(
        id=13,
        name="Python Programming",
        course_code="222",
        section_number=3,
        current_enroll=35,
        max_enroll=30,
        department="CPSC",
        instructor_id=513,
        enrolled=[],
        dropped=[],
    ),
    Class(
        id=14,
        name="Python Programming",
        course_code="222",
        section_number=4,
        current_enroll=44,
        max_enroll=30,
        department="CPSC",
        instructor_id=514,
        enrolled=[],
        dropped=[],
    ),
]

# Enroll students in classes based on current_enroll
place = 1
sid = 1
for class_data in sample_classes:
    while place <= class_data.current_enroll:
        class_data.enrolled.append(sid)
        sid += 1
        place += 1
    place = 1

sample_users = []
for index, user_name in enumerate(names, start=1):
    if index <= 500:
        sample_users.append(User_info(
        id=index,
        name=user_name,
        roles=['student']
        ))
    elif 500 < index <= 550:
        sample_users.append(User_info(
        id=index,
        name=user_name,
        roles=['instructor']
        ))
    else:
        sample_users.append(User_info(
        id=index,
        name=user_name,
        roles=['instructor', 'registrar']
        ))

sample_enrollments = []
place = 1
sid = 1
for index, class_data in enumerate(sample_classes, start = 1):
    while place <= class_data.current_enroll:
        sample_enrollments.append(Enroll(
            placement=place,
            class_id=index,
            student_id=sid
        ))
        sid += 1
        place += 1
    place = 1


#------------------------------------REDIS INITIALIZATION-----------------------------------------------------

# Key patterns
class_waitlist_key = "class:{}:waitlist"
student_waitlists_key = "student:{}:waitlists"


def add_waitlists(class_id, student_id, placement):
    # Add student to class waitlist
    r.zadd(class_waitlist_key.format(class_id), {student_id: placement})

    # Add class to student's waitlist
    r.hset(student_waitlists_key.format(student_id), class_id, placement)


# The following two functions are just used to print all the info from redis
# This is used only for debug purposes
class_waitlist_key_pattern = "class:*:waitlist"
student_waitlists_key_pattern = "student:*:waitlists"


def get_all_class_waitlists():
    keys = r.keys(class_waitlist_key_pattern)
    class_waitlists = {}
    for key in keys:
        class_id = key.decode().split(":")[1]
        waitlist = r.zrange(key, 0, -1, withscores=True)
        class_waitlists[class_id] = waitlist
    return class_waitlists

def get_all_student_waitlists():
    keys = r.keys(student_waitlists_key_pattern)
    student_waitlists = {}
    for key in keys:
        student_id = key.decode().split(":")[1]
        waitlists = r.hgetall(key)
        student_waitlists[student_id] = waitlists
    return student_waitlists


# ---------------------------- Enrollment Initialization ----------------------------------------


def create_database(enrollment, wrapper):
    classes = "class"
    users = "user"
    class_table = table_prefix + classes
    user_table = table_prefix + users
    
    # Check if the tables exist, if they do delete them
    if enrollment.check_table_exists(class_table):
        enrollment.delete_table(classes)
        enrollment.delete_table(users)

    # create the tables
    enrollment.create_table(classes)
    enrollment.create_table(users)

    # initialize the tables with sample data
    for class_data in sample_classes:
        wrapper.run_partiql(
        f"""INSERT INTO \"{class_table}\" VALUE 
        {{
            'id': ?, 
            'name': ?, 
            'course_code': ?, 
            'section_number': ?, 
            'current_enroll': ?, 
            'max_enroll': ?, 
            'department': ?, 
            'instructor_id': ?, 
            'enrolled': ?, 
            'dropped': ?
        }}""",
        [
            class_data.id,
            class_data.name,
            class_data.course_code,
            class_data.section_number,
            class_data.current_enroll,
            class_data.max_enroll,
            class_data.department,
            class_data.instructor_id,
            class_data.enrolled,
            class_data.dropped
        ],
    )
    
    for user_data in sample_users:
        wrapper.run_partiql(
            f"""INSERT INTO \"{user_table}\" VALUE {{'id': ?, 'name': ?, 'roles': ?}}""",
            [user_data.id, user_data.name, user_data.roles],
        )

    # flush all data from the redis db
    r.flushdb()

    # initialize the redis db with waitlist information
    for enrollment_data in sample_enrollments:
        if enrollment_data.placement > 30:
            position = enrollment_data.placement - 30
            add_waitlists(enrollment_data.class_id, enrollment_data.student_id, position)
    
    # add student_id 1 to three different waitlists
    # Used for testing purposes so at least 1 student has max waitlists
    add_waitlists(4, 1, 3)
    add_waitlists(6, 1, 1)
    add_waitlists(8, 1, 1)

    if DEBUG:
        debug_class = []
        debug_user = []
        # Print all classes
        for class_data in sample_classes:
            output = wrapper.run_partiql(
                f'SELECT * FROM "{class_table}" WHERE id=?', [class_data.id]
            )
            debug_class.append(output["Items"])
        print("\nClass Table: \n", debug_class)
    
        # Print all users
        for user_data in sample_users:
            output = wrapper.run_partiql(
                f'SELECT * FROM "{user_table}" WHERE id=?', [user_data.id]
            )
            debug_user.append(output["Items"])
        print("\nUser Table: \n", debug_user)

        all_class_waitlists = get_all_class_waitlists()
        all_student_waitlists = get_all_student_waitlists()

        print("All Class Waitlists:", all_class_waitlists)
        print("All Student Waitlists:", all_student_waitlists)


if __name__ == "__main__":
    try:
        enrollment = Enrollment(dynamodb)
        wrapper = PartiQL(dynamodb)
        create_database(enrollment, wrapper)
    except Exception as e:
        print(f"Something went wrong with the database creation! Here's what: {e}")
