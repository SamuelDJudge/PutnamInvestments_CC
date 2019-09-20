#### Imports ####
import datetime
import time
#################

def new_quarter(quarter: int, year: int, delta_time: str) -> (int, int):
    # the purpose of this function is to input a "current" quarter and year,
    # and shift back either a quarter, year, 3 years, or 5 years.
    # most of these are easy and obvious, but some subtlty with a single quarter
    if delta_time not in ['q', 'y', '3y', '5y']:
        return (10, 0)
    else:
        if delta_time == 'y':
            return (quarter, year-1)
        elif delta_time == '3y':
            return (quarter, year-3)
        elif delta_time == '5y':
            return (quarter, year-5)
        else:
            if quarter == 0:
                # if it is the first quarter of the year, the previous quarter is in the previous year.
                return (3, year - 1)
            else:
                return (quarter - 1, year)

def which_quarter(date: datetime) -> str:
    ## the intent of this function is assign my own numeric system on the data.
    ## rather than dealing with "days", I found it easier to translate everything into
    ## a standardized quarter.
    ### IMPORTANT: note that this is an INTERNAL variable system. For example, for company B,
    ### the assigned Q1 2019 might actually be their Q4 2018, depending on their choice of fiscal year.
    month = date.month
    if month <= 3:
        return 0
    elif month <= 6:
        return 1
    elif month <= 9:
        return 2
    else:
        return 3

def creating_adsh_dict(file_to_open: str) -> (list, dict):

    # The intent of this program is to create a dictionary linking company information based on their ADSH.

    adsh_dict = {}

    with open(file_to_open,'rb') as file:
        # counter is merely here to separate headers from everything else.
        counter = 0
        # error1 has to do with python not being able to read a line. It was rare and could be handled on a
        # case by case basis, but this alerts you if such a problem exists.
        error1 = 0
        for a_line in file:

            try:
                a_line = str(a_line, 'utf-8')
            except:
                error1 += 1

            line_list = a_line.strip().split('\t') # assumption of tab separated. Keep an eye on this.

            adsh = line_list[0]
            cik = line_list[1]

            try:
                name = line_list[2].replace(",", "") # because we are storing in .CSV format, we need to remove commas from the name.
                # NOTE: if you apply this function to European numbers, you will need similiar code to deal with their commas instead of decimal point
            except:
                name = line_list[2]

            filed = line_list[29]

            # is it a header? No. Store the information in a dictionary for quick lookup.
            if counter > 0:
                year = int(filed[:4])
                month = int(filed[4:6])
                day = int(filed[6:])
                filed = datetime.date(year, month, day)

                adsh_dict[adsh] = [cik, name, filed]


            # is it a header? Yes. Store as a list.
            else:
                headers = [cik, name, filed]

            counter += 1
    return headers, adsh_dict

def cleaning_num_file(file_to_open: str, categories: list, company_dict: dict) -> dict:

    headers, adsh_dict = creating_adsh_dict('sub'+ file_to_open[3:])
    # retrieving the headers and dictionary created in the last file.
    # Notice that this assumes that the file_to_open is in the format of
    # num(year)q(quarter).txt where year is a two digit code and quarter is in [1,4]
    # and that the sub file has the format sub(year)q(quarter).txt

    with open(file_to_open, 'rb') as file:
        counter = 0 # counter is here to make sure we separate headers from everything else
        error1 = 0
        # error1 has to do with python not being able to read a line. It was rare and could be handled on a
        # case by case basis, but this alerts you if such a problem exists.
        for a_line in file:

            try: # This try is here to see if the line is readable. If not, we add one to error1.
                a_line = str(a_line, 'utf-8')
                line_list = a_line.split("\t")

                if line_list[1] in sub_categories and len(line_list[3]) == 0:
                    adsh = line_list[0]
                    tag = line_list[1]

                    ddate = line_list[4]
                    # this is to separate the string yyyymmdd into its individual parts to put into a datetime object
                    year = int(ddate[:4])
                    month = int(ddate[4:6])
                    day = int(ddate[6:])
                    ddate = datetime.date(year, month, day)

                    # this is to create my own "numeric" quarter system. This will allow for me to quickly go back "1 quarter"
                    # rather than try to figure out if that quarter is 90, 91, 92, or 93 days.
                    quarter = which_quarter(ddate)
                    new_year = ddate.year

                    qtrs = line_list[5]
                    uom = line_list[6]
                    value = line_list[7]

                    # this is associating the information with the correct company via the adsh_dict.
                    # quick lookup makes this part very fast.

                    more_terms = adsh_dict[adsh]
                    cik = more_terms[0]
                    name = more_terms[1]
                    filed = more_terms[2]

                    # we are creating company_dict, which will have the schema
                    # company_dict[cik] = [name, {year: {quarter: [value, ddate, filed]}}]
                    # it will do this for all unique (year, quarter) filings made by a company

                    #if cik == '1518720':
                    # uncommenting the above (and commenting out below) and putting in the appropriate CIK will allow you to run the program on a single company
                    if True:

                        try:
                            company_dict[cik] # check to see if this CIK already exists in the dictionary

                            try:
                                company_dict[cik][1][new_year] # if it does, check to see if it already has your year

                                try:
                                    filed_date = company_dict[cik][1][new_year][quarter][2] # if it has the two above, check to see if it also has the correct quarter
                                    # if yes, check the filed date.
                                    # If you get to this point, this is a "repeated data point". Check to see which is filed more recently. Keep that one.
                                    if filed > filed_date:
                                        company_dict[cik][1][new_year][quarter] = [value, ddate, filed]

                                except: # if it does not have the correct quarter, add it
                                    company_dict[cik][1][new_year][quarter] = [value, ddate, filed]

                            except: # if it does not have the correct year, add it.
                                company_dict[cik][1][new_year] = {quarter: [value, ddate, filed]}


                        except: # if it does not have your CIK (or company), add it.
                            company_dict[cik] = [name, {new_year: {quarter: [value, ddate, filed]}}]


            except:
                error1 += 1 # if you cannot read a line (or have some other error), add one to error1.
            counter += 1
    print("There were ",error1," lines that produced errors in ",file_to_open) # just a read out of what the problem files are.
    return company_dict

def writing_to_file(company_dict: dict, write_file_name: str, tag: str, begin_year: int, end_year: int) -> None:
    # the intention of this program is to calculate and write all the results to a file.
    for a_quarter in range(4):
        for a_year in range(begin_year, end_year + 1):
            for a_key in company_dict.keys():

                name = company_dict[a_key][0]

                csv_string = a_key + ", " + name + ", " # creating the line we will ultimately write to the CSV.

                try:
                    info_list = company_dict[a_key][1][a_year][a_quarter][:] # checking to see, for every company, if that company filed in that quarter and year.

                    value = float(info_list[0]) # if they did, here's the value they stated

                    ddate = info_list[1]
                    csv_string += str(ddate) + ", " # writing to the line

                    csv_string += tag + ", " # writing to the line

                    try:
                        old_values = []
                        for a_term in ['q','y','3y','5y']:
                            new_q, new_y = new_quarter(a_quarter, a_year, a_term) # finding what the correct distance "back" to check is
                            try:
                                old_value = float(company_dict[a_key][1][new_y][new_q][0]) # checking to see if that filing exists
                            except:
                                old_value = None
                            old_values.append(old_value)

                        counter = 0
                        for a_value in old_values:
                            if a_value == None or a_value == 0:
                                csv_string += ", "
                                counter += 1
                            else:
                                percentage = round((value - a_value) / a_value * 100 , 2) # for each filing that exists, calculate the percentage
                                csv_string += str(percentage) + ", " # add that percentage, in order, to the written line.
                    except:
                        x = None # just a place holder. I want nothing to happen if I hit this except.

                    if counter < 4: # this confirms that at least one of the four values exists. Otherwise, there is no point in writing it.
                        csv_string += "\n"
                        file = open(write_file_name ,'a')
                        file.writelines(csv_string)
                        file.close()



                except:
                    x = None # just a place holder. I want nothing to happen if I hit this except.

    return

if __name__ == "__main__":

    years = [2010,2019] # beginning and end year, inclusive.

    open_files = ['num10q1.txt', 'num10q2.txt', 'num10q3.txt', 'num10q4.txt', 'num11q1.txt', 'num11q2.txt', 'num11q3.txt', 'num11q4.txt', 'num12q1.txt', 'num12q2.txt', 'num12q3.txt', 'num12q4.txt', 'num13q1.txt', 'num13q2.txt', 'num13q3.txt', 'num13q4.txt', 'num14q1.txt', 'num14q2.txt', 'num14q3.txt', 'num14q4.txt', 'num15q1.txt', 'num15q2.txt', 'num15q3.txt', 'num15q4.txt', 'num16q1.txt', 'num16q2.txt', 'num16q3.txt', 'num16q4.txt', 'num17q1.txt', 'num17q2.txt', 'num17q3.txt', 'num17q4.txt', 'num18q1.txt', 'num18q2.txt', 'num18q3.txt', 'num18q4.txt', 'num19q1.txt', 'num19q2.txt'] # write the file names here. Note that it is assuming a specific format.


    sub_categories = ['Assets'] # Enter the categories you are interested in. Note that this calcuation is assuming "moment in time," so be careful about "netchange"

    write_file_name = "percentage_growth.csv" # Choose the name of the output file. Problem statement specified it should be .csv.

    headers = "CIK, NAME, DDATE, MEASURE, QOQ_GROWTH, YOY_GROWTH, 3Y_GROWTH, 5Y_GROWTH\n" # Writing the headers to the file.

    file = open(write_file_name ,'a')
    file.writelines(headers)
    file.close()

    t = time.time() # Seeing how long the functions take to write.

    companies_dict = {} # Setting the intial empty dictionary.

    for a_category in sub_categories:
        for a_file in open_files:
            try:
                companies_dict = cleaning_num_file(a_file, [a_category], companies_dict).copy() # updating the dictionary.
            except:
                print(a_file," does not exist.") # this will happen if it cannot find the file.

        print("The dictionary was created in ",time.time()-t, " seconds.")

        t = time.time()

        writing_to_file(companies_dict, write_file_name, a_category, years[0], years[1]) # writing the dictionary to the file.

        print("The file was written in ",time.time()-t, " seconds.")
