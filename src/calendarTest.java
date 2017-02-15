import java.util.Calendar;

public class calendarTest {

	public static void main(String[] args){
		
	   System.out.println("Dividendo i giorni");
	   
	   Calendar cal = Calendar.getInstance();
	   
	   cal.add(Calendar.DATE, 0);
	   System.out.println("Anno: " + cal.get(Calendar.YEAR));
		for(int i=1; i<=45; i++){
			cal.add(Calendar.DATE, -1);
			int day_of_year = cal.get(Calendar.DAY_OF_YEAR);
			int week_of_year;
			if(day_of_year % 7 == 0){
				week_of_year = (day_of_year/7);
			}else{
				week_of_year = (day_of_year/7) + 1;
			}
			
			int month = cal.get(Calendar.MONTH) + 1;
			System.out.println(cal.get(Calendar.DAY_OF_MONTH) + "/" + month + "->" + week_of_year + "->" + day_of_year);
		}

	}
}
